#include "postgres_fe.h"
#include "common/zpq_stream.h"
#include "c.h"
#include "pg_config.h"
#include "port/pg_bswap.h"
#include "common/z_stream.h"

#define ZPQ_BUFFER_SIZE       81920 /* We have to flush stream after each
									 * protocol command and command is mostly
									 * limited by record length, which in turn
									 * is usually less than page size (except
									 * TOAST) */

typedef struct ZpqBuffer ZpqBuffer;

struct ZpqBuffer {
    char    buf[ZPQ_BUFFER_SIZE];
    size_t  pos;
    size_t  pos_read;
};

static inline void zpq_buf_init(ZpqBuffer *zb) {
    zb->pos = 0;
    zb->pos_read = 0;
}

static inline size_t zpq_buf_left(ZpqBuffer *zb)
{
    Assert(zb->buf);
    return ZPQ_BUFFER_SIZE - zb->pos;
}

static inline size_t zpq_buf_unread(ZpqBuffer *zb)
{
    return zb->pos - zb->pos_read;
}

static inline char *zpq_buf_pos(ZpqBuffer *zb)
{
    return (char *)(zb->buf) + zb->pos;
}

static inline char *zpq_buf_read(ZpqBuffer *zb)
{
    return (char *)(zb->buf) + zb->pos_read;
}

static inline void *zpq_buf_pos_advance(ZpqBuffer *zb, size_t value)
{
    zb->pos += value;
}

static inline void *zpq_buf_pos_read_advance(ZpqBuffer *zb, size_t value)
{
    zb->pos_read += value;
}

static inline void zpq_buf_reuse(ZpqBuffer *zb)
{
    size_t unread = zpq_buf_unread(zb);
    if (unread > 5) /* can read message header, don't do anything */
        return;
    if (unread == 0) {
        zb->pos = 0;
        zb->pos_read = 0;
        return;
    }
    memmove(zb->buf, zb->buf + zb->pos_read, unread);
    zb->pos = unread;
    zb->pos_read = 0;
}

struct ZpqStream
{
	ZStream    *z_stream; /* underlying compression stream */
    char		tx_msg_h_buf[5]; /* incomplete message header buffer */
    size_t		tx_msg_h_size;
    size_t      tx_msg_h_pos;

	size_t		tx_total; /* amount of bytes sent to tx_func */
	size_t		tx_total_raw; /* amount of bytes received by zpq_write */
	size_t		rx_total; /* amount of bytes read by rx_func */
	size_t		rx_total_raw; /* amount of bytes returned by zpq_write */

	bool		is_compressing;
	bool		is_decompressing;

	size_t		rx_msg_bytes_left;
	size_t		tx_msg_bytes_left;

	ZpqBuffer   rx; /* buffer for readahead data read by rx_func */
	ZpqBuffer   tx; /* buffer for data waiting for send via tx_func */

	zpq_rx_func rx_func;
	zpq_tx_func tx_func;
	void	   *arg;
};

/*
 * Check if should compress message of msg_type with msg_len.
 * Return true if should, false if should not.
 */
static inline bool zpq_should_compress(char msg_type, uint32 msg_len){
    return msg_type == 'd' || msg_type == 'D' || msg_len > 60; /* subject to change? */
}

/*
 * Check if message is a CompressedMessage.
 * Return true if it is, otherwise false.
 * */
static inline bool zpq_is_compressed_message(char msg_type) {
    return msg_type == 'm';
}

ZpqStream *
zpq_create(int c_alg_impl, int c_level, int d_alg_impl, zpq_tx_func tx_func, zpq_rx_func rx_func, void *arg, char *rx_data, size_t rx_data_size)
{
	ZpqStream  *zc = (ZpqStream *) malloc(sizeof(ZpqStream));

	zc->is_compressing = false;
	zc->is_decompressing = false;
	zc->rx_msg_bytes_left = 0;
	zc->tx_msg_bytes_left = 0;
	zc->tx_msg_h_size = 0;
	zc->tx_msg_h_pos = 0;

	zc->tx_total = 0;
	zc->tx_total_raw = 0;
	zc->rx_total = 0;
	zc->rx_total_raw = 0;

	zpq_buf_init(&zc->rx);
    zpq_buf_pos_advance(&zc->rx, rx_data_size);
	Assert(rx_data_size < ZPQ_BUFFER_SIZE);
	memcpy(zc->rx.buf, rx_data, rx_data_size);

    zpq_buf_init(&zc->tx);

	zc->rx_func = rx_func;
	zc->tx_func = tx_func;
	zc->arg = arg;

	zc->z_stream = zs_create(c_alg_impl, c_level, d_alg_impl);
	if (zc->z_stream == NULL)
	{
		free(zc);
		return NULL;
	}
	return zc;
}

/* Compress up to src_size bytes from *src into CompressedMessage and write it to the tx buffer.
 * Returns ZS_OK on success, ZS_COMPRESS_ERROR if encountered a compression error. */
static inline ssize_t
zpq_write_compressed_message(ZpqStream * zc, char const *src, size_t src_size, size_t *src_processed)
{
    size_t		compressed_len;
    ssize_t		rc;
    uint32		size;

	/* check if have enough space */
	if (zpq_buf_left(&zc->tx) <= 5)
	{
		/* too little space for CompressedMessage, abort */
		*src_processed = 0;
		return ZS_OK;
	}

	compressed_len = 0;
    rc = zs_write(zc->z_stream, src, src_size, src_processed,
                  zpq_buf_pos(&zc->tx) + 5, zpq_buf_left(&zc->tx) - 5, &compressed_len);

	if (compressed_len > 0)
	{
        *zpq_buf_pos(&zc->tx) = 'm'; /* write CompressedMessage type */
		size = pg_hton32(compressed_len + 4);

		memcpy(zpq_buf_pos(&zc->tx) + 1, &size, sizeof(uint32));	/* write msg length */
		compressed_len += 5; /* append header length to compressed data length */
	}

	zc->tx_total_raw += *src_processed;
	zc->tx_total += compressed_len;

	zpq_buf_pos_advance(&zc->tx, compressed_len);
	return rc;
}

/* Copy the data directly from *src to the tx buffer */
static void
zpq_write_uncompressed(ZpqStream * zc, char const *src, size_t src_size, size_t *src_processed)
{
	src_size = Min(zpq_buf_left(&zc->tx), src_size);
	memcpy(zpq_buf_pos(&zc->tx), src, src_size);

	zc->tx_total_raw += src_size;
	zc->tx_total += src_size;
	zpq_buf_pos_advance(&zc->tx, src_size);
	*src_processed = src_size;
}

/* Determine if should compress the next message and
 * change the current compression state */
static ssize_t
zpq_toggle_compression(ZpqStream * zpq, char msg_type, uint32 msg_len) {
    if (zpq_should_compress(msg_type, msg_len))
    {
        zpq->is_compressing = true;
    }
    else if (zpq->is_compressing)
    {
        /* Won't compress the next message, should now finish the compression.
         * Make sure there is no buffered data left in underlying compression stream */
        while (zs_buffered_tx(zpq->z_stream))
        {
            size_t		flushed_len = 0;
            ssize_t		flush_rc = zpq_write_compressed_message(zpq, NULL, 0, &flushed_len);

            if (flush_rc != ZS_OK)
            {
                return flush_rc;
            }
        }
        zpq->is_compressing = false;
    }
    zpq->tx_msg_bytes_left = msg_len + 1;
    return 0;
}

/*
 * Internal write function. Reads the data from *src buffer,
 * determines the postgres messages type and length.
 * If message matches the compression criteria, it wraps the message into
 * CompressedMessage. Otherwise, leaves the message unchanged.
 * If *src data ends with incomplete message header, this function is not
 * going to read this message header.
 */
static ssize_t zpq_write_internal(ZpqStream * zq, void const *src, size_t src_size, size_t *processed)  {
    size_t		src_pos = 0;
    ssize_t		rc;

    do
    {
        /* to reduce the socket write calls count and increase average payload length, do not call tx_func until absolutely necessary.
         * should_send_now is true if there is no buffered data left in compression buffers and one of the following is true:
         * 1. Processed all of the *src data
         * 2. Have some *src data (<5 bytes) left unprocessed but can't process it because full message header is needed to determine tx_msg_bytes_left size.
         */
        bool should_send_now = (src_size - src_pos == 0 || (src_size - src_pos < 5 && zq->tx_msg_bytes_left == 0)) && !zs_buffered_tx(zq->z_stream);

        /* call the tx_func if should_send_now or too few space left in TX buffer (<16 bytes) */
        while (zpq_buf_unread(&zq->tx) && (should_send_now || zpq_buf_left(&zq->tx) < 16))
        {
            rc = zq->tx_func(zq->arg, zpq_buf_read(&zq->tx), zpq_buf_unread(&zq->tx));
            if (rc > 0)
            {
                zpq_buf_pos_read_advance(&zq->tx, rc);
            }
            else
            {
                *processed = src_pos;
                return rc;
            }
        }
        zpq_buf_reuse(&zq->tx);
        if (!zpq_buffered_tx(zq) && src_size - src_pos == 0)
        {
            /* don't have anything to process, do not proceed further */
            break;
        }

        /*
         * try to read ahead the next message types and increase
         * tx_msg_bytes_left, if possible
         */
        while (zq->tx_msg_bytes_left > 0 && src_size - src_pos >= zq->tx_msg_bytes_left + 5)
        {
            char		msg_type = *((char *) src + src_pos + zq->tx_msg_bytes_left);
            uint32		msg_len;

            memcpy(&msg_len, (char *) src + src_pos + zq->tx_msg_bytes_left + 1, 4);
            msg_len = pg_ntoh32(msg_len);
            if (zpq_should_compress(msg_type, msg_len) != zq->is_compressing)
            {
                /*
                 * cannot proceed further, encountered compression toggle
                 * point
                 */
                break;
            }
            zq->tx_msg_bytes_left += msg_len + 1;
        }

        /* Write CompressedMessage if currently is compressing or
         * have some buffered data left in underlying compression stream */
        if (zs_buffered_tx(zq->z_stream) || (zq->is_compressing && zq->tx_msg_bytes_left > 0))
        {
            size_t		buf_processed = 0;
            size_t		to_compress = Min(zq->tx_msg_bytes_left, src_size - src_pos);

            rc = zpq_write_compressed_message(zq, (char *) src + src_pos, to_compress, &buf_processed);
            src_pos += buf_processed;
            zq->tx_msg_bytes_left -= buf_processed;

            if (rc != ZS_OK)
            {
                *processed = src_pos;
                return rc;
            }
        }
        /* If not going to compress the data from *src, just write it uncompressed. */
        else if (zq->tx_msg_bytes_left > 0)
        {						/* determine next message type */
            size_t		copy_len = Min(src_size - src_pos, zq->tx_msg_bytes_left);
            size_t		copy_processed = 0;

            zpq_write_uncompressed(zq, (char *) src + src_pos, copy_len, &copy_processed);
            src_pos += copy_processed;
            zq->tx_msg_bytes_left -= copy_processed;
        }
        /* Reached the compression toggle point, fetch next message header
         * to determine compression state. */
        else
        {
            char msg_type;
            uint32 msg_len;

            if (src_size - src_pos < 5) {
                /* must return here because we can't continue
                 * without full message header */
                return src_pos;
            }

            msg_type = *((char*)src + src_pos);
            memcpy(&msg_len, (char*)src + src_pos + 1, 4);
            msg_len = pg_ntoh32(msg_len);
            rc = zpq_toggle_compression(zq, msg_type, msg_len);
            if (rc) {
                return rc;
            }
        }
        /*
         * repeat sending while there is some data in input or internal
         * compression buffer
         */
    } while (src_pos < src_size);

    return src_pos;
}

ssize_t
zpq_write(ZpqStream * zq, void const *src, size_t src_size, size_t *src_processed)
{
    size_t src_pos = 0;
    ssize_t rc;
    do {
        /* reset the incomplete message header buffer if it has been processed */
        if (zq->tx_msg_h_pos == zq->tx_msg_h_size) {
            Assert(zq->tx_msg_h_size == 0 || zq->tx_msg_h_size == 5);
            zq->tx_msg_h_pos = zq->tx_msg_h_size = 0;
        }

        if ((zq->tx_msg_bytes_left > 0 || src_size - src_pos >= 5 || zpq_buffered_tx(zq)) && zq->tx_msg_h_size == 0) {
            rc = zpq_write_internal(zq, (char *) src + src_pos, src_size - src_pos, src_processed);
            if (rc > 0) {
                rc += src_pos;
            } else {
                *src_processed += src_pos;
            }
            return rc;
        }

        if (zq->tx_msg_h_size < 5) {
            /* read more data into incomplete header buffer */
            size_t		to_copy = Min(5 - zq->tx_msg_h_size, src_size - src_pos);
            memcpy((char *) zq->tx_msg_h_buf + zq->tx_msg_h_size, (char*)src + src_pos, to_copy);
            zq->tx_msg_h_size += to_copy;
            src_pos += to_copy;
            if (zq->tx_msg_h_size < 5) {
                /* message header is still incomplete, can't proceed further */
                Assert(src_size - src_pos == 0);
                return src_pos;
            }
        }

        Assert(zq->tx_msg_h_size == 5);
        rc = zpq_write_internal(zq, (char *) zq->tx_msg_h_buf + zq->tx_msg_h_pos, zq->tx_msg_h_size - zq->tx_msg_h_pos,
                                src_processed);
        if (rc > 0) {
            zq->tx_msg_h_pos += rc;
        } else {
            zq->tx_msg_h_pos += *src_processed;
            *src_processed = src_pos;
            return rc;
        }
    } while (src_pos < src_size || zpq_buffered_rx(zq));
    return src_pos;
}


/* Decompress bytes from RX buffer and write up to dst_len of uncompressed data to *dst.
 * Returns:
 * ZS_OK on success,
 * ZS_STREAM_END if reached end of compressed chunk
 * ZS_DECOMPRESS_ERROR if encountered a decompression error */
static inline ssize_t
zpq_read_compressed_message(ZpqStream * zc, char *dst, size_t dst_len, size_t *dst_processed) {
    size_t rx_processed = 0;
    ssize_t rc;
    size_t		read_len = Min(zc->rx_msg_bytes_left, zpq_buf_unread(&zc->rx));

    rc = zs_read(zc->z_stream, zpq_buf_read(&zc->rx), read_len, &rx_processed,
                 dst, dst_len, dst_processed);

    zpq_buf_pos_read_advance(&zc->rx, rx_processed);
    zc->rx_total_raw += *dst_processed;
    zc->rx_msg_bytes_left -= rx_processed;
    return rc;
}

/* Copy up to dst_len bytes from rx buffer to *dst */
static inline size_t
zpq_read_uncompressed(ZpqStream * zc, char *dst, size_t dst_len) {
    Assert(zpq_buf_unread(&zc->rx) > 0);
    size_t		copy_len = Min(zc->rx_msg_bytes_left, Min(zpq_buf_unread(&zc->rx), dst_len));
    memcpy(dst, zpq_buf_read(&zc->rx), copy_len);

    zpq_buf_pos_read_advance(&zc->rx, copy_len);
    zc->rx_total_raw += copy_len;
    zc->rx_msg_bytes_left -= copy_len;
    return copy_len;
}

/* Determine if should decompress the next message and
 * change the current decompression state */
static inline void
zpq_toggle_decompression(ZpqStream * zc) {
    uint32		msg_len;
    char		msg_type = *zpq_buf_read(&zc->rx);

    zc->is_decompressing = zpq_is_compressed_message(msg_type);

    memcpy(&msg_len, zpq_buf_read(&zc->rx) + 1, 4);
    zc->rx_msg_bytes_left = pg_ntoh32(msg_len) + 1;

    if (zc->is_decompressing)
    {
        /* compressed message header is no longer needed, just skip it */
        zpq_buf_pos_read_advance(&zc->rx, 5);
        zc->rx_msg_bytes_left -= 5;
    }
}

ssize_t
zpq_read(ZpqStream * zc, void *buf, size_t size)
{
	size_t		buf_pos = 0;
	size_t		buf_processed = 0;
	ssize_t		rc;

	/* Read until some data fetched */
	while (buf_pos == 0)
	{
	    zpq_buf_reuse(&zc->rx);

		if (!zpq_buffered_rx(zc))
		{
			rc = zc->rx_func(zc->arg, zpq_buf_pos(&zc->rx), zpq_buf_left(&zc->rx));
			if (rc > 0)			/* read fetches some data */
			{
				zc->rx_total += rc;
				zpq_buf_pos_advance(&zc->rx, rc);
			}
			else				/* read failed */
			{
				return rc;
			}
		}

		/*
		 * try to read ahead the next message types and increase
		 * rx_msg_bytes_left, if possible
		 */
		while (zc->rx_msg_bytes_left > 0 && (zpq_buf_unread(&zc->rx) >= zc->rx_msg_bytes_left + 5))
		{
			char		msg_type;

			msg_type = *(zpq_buf_read(&zc->rx) + zc->rx_msg_bytes_left);
			if (zc->is_decompressing || zpq_is_compressed_message(msg_type))
			{
				/*
				 * cannot proceed further, encountered compression toggle
				 * point
				 */
				break;
			}
			uint32		msg_len;

			memcpy(&msg_len, zpq_buf_read(&zc->rx) + zc->rx_msg_bytes_left + 1, 4);
			zc->rx_msg_bytes_left += pg_ntoh32(msg_len) + 1;
		}


		if (zc->rx_msg_bytes_left > 0 || zs_buffered_rx(zc->z_stream))
		{
            buf_processed = 0;
			if (zc->is_decompressing || zs_buffered_rx(zc->z_stream))
			{
				rc = zpq_read_compressed_message(zc, buf, size-buf_pos, &buf_processed);
				buf_pos += buf_processed;
				if (rc == ZS_STREAM_END)
				{
					continue;
				}
				if (rc != ZS_OK)
				{
					return rc;
				}
			}
			else
                buf_pos += zpq_read_uncompressed(zc, zpq_buf_read(&zc->rx), size - buf_pos);
		}
		else if (zpq_buf_unread(&zc->rx) >= 5)
            zpq_toggle_decompression(zc);
	}
	return buf_pos;
}

bool
zpq_buffered_rx(ZpqStream * zc)
{
	return zc ? zpq_buf_unread(&zc->rx) >= 5 || (zpq_buf_unread(&zc->rx) > 0 && zc->rx_msg_bytes_left > 0) || zs_buffered_rx(zc->z_stream) : 0;
}

bool
zpq_buffered_tx(ZpqStream * zc)
{
	return zc ? zc->tx_msg_h_size == 5 || (zc->tx_msg_h_size > 0 && zc->tx_msg_bytes_left > 0) || zpq_buf_unread(&zc->tx) > 0 ||
		zs_buffered_tx(zc->z_stream) : 0;
}

void
zpq_free(ZpqStream * zc)
{
	if (zc)
	{
		if (zc->z_stream)
		{
			zs_free(zc->z_stream);
		}
		free(zc);
	}
}

char const *
zpq_compress_error(ZpqStream * zc)
{
	return zs_compress_error(zc->z_stream);
}

char const *
zpq_decompress_error(ZpqStream * zc)
{
	return zs_decompress_error(zc->z_stream);
}

char const *
zpq_compress_algorithm_name(ZpqStream * zc)
{
	return zs_compress_algorithm_name(zc->z_stream);
}
