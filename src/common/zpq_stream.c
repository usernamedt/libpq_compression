#include "postgres_fe.h"
#include "common/zpq_stream.h"
#include "c.h"
#include "pg_config.h"
#include "port/pg_bswap.h"
#include "common/z_stream.h"

/* Check if should compress provided msg_type.
 * Return true if should, false if should not.
 */
#define zpq_should_compress(msg_type, msg_len) (msg_type == 'd' || msg_type == 'D' || msg_len > 60)

#define zpq_should_decompress(msg_type) (msg_type == 'm')


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

static inline ssize_t zpq_buf_unread(ZpqBuffer *zb)
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
    char		tx_msg_h_buf[5];/* incomplete header buffer */
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
		zpq_free(zc);
		return NULL;
	}
	return zc;
}

static ssize_t
zpq_write_compressed(ZpqStream * zc, char const *src, size_t src_size, size_t *src_processed)
{
	/* check if have some space */
	if (zpq_buf_left(&zc->tx) <= 5)
	{
		/* too little space for compressed message, just return */
		*src_processed = 0;
		return ZS_OK;
	}

	size_t		compressed_len;
	ssize_t		rc = zs_write(zc->z_stream, src, src_size, src_processed,
                        zpq_buf_pos(&zc->tx) + 5, zpq_buf_left(&zc->tx) - 5, &compressed_len);

	if (compressed_len > 0)
	{
        *zpq_buf_pos(&zc->tx) = 'm'; /* write compressed
													 * message type */
		uint32		size = pg_hton32(compressed_len + 4);

		memcpy(zpq_buf_pos(&zc->tx) + 1, &size, sizeof(uint32));	/* write compressed
																				 * message length */
		compressed_len += 5;
	}

	zc->tx_total_raw += *src_processed;
	zc->tx_total += compressed_len;

	zpq_buf_pos_advance(&zc->tx, compressed_len);
	return rc;
}

static void
zpq_write_raw(ZpqStream * zc, char const *src, size_t src_size, size_t *src_processed)
{
	src_size = Min(zpq_buf_left(&zc->tx), src_size);

	memcpy(zpq_buf_pos(&zc->tx), src, src_size);

	zc->tx_total_raw += src_size;
	zc->tx_total += src_size;
	zpq_buf_pos_advance(&zc->tx, src_size);
	*src_processed = src_size;
}

static ssize_t
zpq_switch_compression(ZpqStream * zpq, char msg_type, uint32 msg_len) {
    if (zpq_should_compress(msg_type, msg_len))
    {
        zpq->is_compressing = true;
    }
    else if (zpq->is_compressing)
    {
        while (zs_buffered_tx(zpq->z_stream))	/* make sure there is no
                                                 * buffered data left in
                                                 * compressor */
        {
            size_t		flush_processed = 0;
            ssize_t		flush_rc = zpq_write_compressed(zpq, NULL, 0, &flush_processed);

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

static ssize_t zpq_write_internal(ZpqStream * zq, void const *buf, size_t size, size_t *processed)  {
    size_t		buf_pos = 0;
    ssize_t		rc;

    do
    {
        /*
         * send all pending data if no more free space in ZPQ buffer (< 16
         * bytes) or don't have any non-processed data left
         */
        while (zpq_buf_unread(&zq->tx) &&
               (((size - buf_pos == 0 || (size - buf_pos < 5 && zq->tx_msg_bytes_left == 0)) && !zs_buffered_tx(zq->z_stream)) || zpq_buf_left(&zq->tx) < 16))
        {
            rc = zq->tx_func(zq->arg, zpq_buf_read(&zq->tx), zpq_buf_unread(&zq->tx));

            if (rc > 0)
            {
                zpq_buf_pos_read_advance(&zq->tx, rc);
            }
            else
            {
                *processed = buf_pos;
                return rc;
            }
        }
        zpq_buf_reuse(&zq->tx);

        if (!zpq_buffered_tx(zq) && size - buf_pos == 0)
        {
            continue;			/* don't have anything to process, do not
								 * proceed further */
        }

        /*
         * try to read ahead the next message types and increase
         * tx_msg_bytes_left, if possible
         */
        while (zq->tx_msg_bytes_left > 0 && size - buf_pos >= zq->tx_msg_bytes_left + 5)
        {
            char		msg_type = *((char *) buf + buf_pos + zq->tx_msg_bytes_left);
            uint32		msg_len;

            memcpy(&msg_len, (char *) buf + buf_pos + zq->tx_msg_bytes_left + 1, 4);
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

        if (zs_buffered_tx(zq->z_stream) || (zq->is_compressing && zq->tx_msg_bytes_left > 0))
        {
            size_t		buf_processed = 0;
            size_t		to_compress = Min(zq->tx_msg_bytes_left, size - buf_pos);

            rc = zpq_write_compressed(zq, (char *) buf + buf_pos, to_compress, &buf_processed);
            buf_pos += buf_processed;
            zq->tx_msg_bytes_left -= buf_processed;

            if (rc != ZS_OK)
            {
                *processed = buf_pos;
                return rc;
            }
        }
        else if (zq->tx_msg_bytes_left > 0)
        {						/* determine next message type */
            size_t		copy_len = Min(size - buf_pos, zq->tx_msg_bytes_left);
            size_t		copy_processed = 0;

            zpq_write_raw(zq, (char *) buf + buf_pos, copy_len, &copy_processed);
            buf_pos += copy_processed;
            zq->tx_msg_bytes_left -= copy_processed;
        }
        else
        {
            char msg_type;
            uint32 msg_len;

            if (size - buf_pos < 5) {
                /* must return here because we can't continue
                 * without full message header */
                return buf_pos;
            }

            msg_type = *((char*)buf + buf_pos);
            memcpy(&msg_len, (char*)buf + buf_pos + 1, 4);
            msg_len = pg_ntoh32(msg_len);
            rc = zpq_switch_compression(zq, msg_type, msg_len);
            if (rc) {
                return rc;
            }
        }

        /*
         * repeat sending while there is some data in input or internal
         * compression buffer
         */
    } while (buf_pos < size);

    return buf_pos;
}

ssize_t
zpq_write(ZpqStream * zq, void const *buf, size_t size, size_t *processed)
{
    size_t buf_pos = 0;
    ssize_t rc;
    do {
        if (zq->tx_msg_h_pos == zq->tx_msg_h_size) {
            Assert(zq->tx_msg_h_size == 0 || zq->tx_msg_h_size == 5);
            zq->tx_msg_h_pos = zq->tx_msg_h_size = 0;
        }

        if ((zq->tx_msg_bytes_left > 0 || zpq_buffered_tx(zq) || size - buf_pos >= 5) && zq->tx_msg_h_size == 0) {
            rc = zpq_write_internal(zq, (char *) buf + buf_pos, size - buf_pos, processed);
            if (rc > 0) {
                rc += buf_pos;
            } else {
                *processed += buf_pos;
            }
            return rc;
        }

        if (zq->tx_msg_h_size < 5) {
            /* read more data into incomplete header buffer */
            size_t		to_copy = Min(5 - zq->tx_msg_h_size, size - buf_pos);
            memcpy((char *) zq->tx_msg_h_buf + zq->tx_msg_h_size, (char*)buf + buf_pos, to_copy);
            zq->tx_msg_h_size += to_copy;
            buf_pos += to_copy;
            if (zq->tx_msg_h_size < 5) {
                /* message header is still incomplete, can't proceed further */
                Assert(size - buf_pos == 0);
                return buf_pos;
            }
        }

        Assert(zq->tx_msg_h_size == 5);
        rc = zpq_write_internal(zq, (char *) zq->tx_msg_h_buf + zq->tx_msg_h_pos, zq->tx_msg_h_size - zq->tx_msg_h_pos,
                                processed);
        if (rc > 0) {
            zq->tx_msg_h_pos += rc;
        } else {
            zq->tx_msg_h_pos += *processed;
            *processed = buf_pos;
            return rc;
        }
    } while (buf_pos < size);
    return buf_pos;
}

ssize_t
zpq_read(ZpqStream * zc, void *buf, size_t size)
{
	size_t		buf_pos = 0;
	size_t		rx_processed;
	size_t		buf_processed;
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
			if (zc->is_decompressing || zpq_should_decompress(msg_type))
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
			if (zc->is_decompressing || zs_buffered_rx(zc->z_stream))
			{
				rx_processed = 0;
				buf_processed = 0;

				size_t		read_count = Min(zc->rx_msg_bytes_left, zpq_buf_unread(&zc->rx));

				rc = zs_read(zc->z_stream, zpq_buf_read(&zc->rx), read_count, &rx_processed,
                             buf, size - buf_pos, &buf_processed);

				zpq_buf_pos_read_advance(&zc->rx, rx_processed);
				zc->rx_total_raw += buf_processed;
				buf_pos += buf_processed;
				zc->rx_msg_bytes_left -= rx_processed;
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
			{
				Assert(zpq_buf_unread(&zc->rx) > 0);
				size_t		copy_len = Min(zc->rx_msg_bytes_left, Min(zpq_buf_unread(&zc->rx), size - buf_pos));
				memcpy(buf, zpq_buf_read(&zc->rx), copy_len);

				zpq_buf_pos_read_advance(&zc->rx, copy_len);
				buf_pos += copy_len;
				zc->rx_total_raw += copy_len;
				zc->rx_msg_bytes_left -= copy_len;
			}
		}
		else if (zpq_buf_unread(&zc->rx) >= 5)
		{						/* determine next message type */
			uint32		msg_len;

			/* read msg type and length if possible */
			char		msg_type = *zpq_buf_read(&zc->rx);

			zc->is_decompressing = zpq_should_decompress(msg_type);

			memcpy(&msg_len, zpq_buf_pos(&zc->rx) + 1, 4);
			zc->rx_msg_bytes_left = pg_ntoh32(msg_len) + 1;

			if (zc->is_decompressing)
			{
				/* compressed message header is no longer needed, just skip it */
				zpq_buf_pos_read_advance(&zc->rx, 5);
				zc->rx_msg_bytes_left -= 5;
			}
		}
	}
	return buf_pos;
}

size_t
zpq_buffered_rx(ZpqStream * zc)
{
	return zc ? zpq_buf_unread(&zc->rx) >= 5 || (zpq_buf_unread(&zc->rx) > 0 && zc->rx_msg_bytes_left > 0) || zs_buffered_rx(zc->z_stream) : 0;
}

size_t
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
