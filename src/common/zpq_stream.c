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
#define ZPQ_COMPRESSED_MSG_TYPE 'm'

typedef struct ZpqBuffer ZpqBuffer;

struct ZpqBuffer
{
	char		buf[ZPQ_BUFFER_SIZE];
	size_t		size;
	size_t		pos;
};

static inline void
zpq_buf_init(ZpqBuffer * zb)
{
	zb->size = 0;
	zb->pos = 0;
}

static inline size_t
zpq_buf_left(ZpqBuffer * zb)
{
	Assert(zb->buf);
	return ZPQ_BUFFER_SIZE - zb->size;
}

static inline size_t
zpq_buf_unread(ZpqBuffer * zb)
{
	return zb->size - zb->pos;
}

static inline char *
zpq_buf_size(ZpqBuffer * zb)
{
	return (char *) (zb->buf) + zb->size;
}

static inline char *
zpq_buf_pos(ZpqBuffer * zb)
{
	return (char *) (zb->buf) + zb->pos;
}

static inline void *
zpq_buf_size_advance(ZpqBuffer * zb, size_t value)
{
	zb->size += value;
}

static inline void *
zpq_buf_pos_advance(ZpqBuffer * zb, size_t value)
{
	zb->pos += value;
}

static inline void
zpq_buf_reuse(ZpqBuffer * zb)
{
	size_t		unread = zpq_buf_unread(zb);

	if (unread > 5)				/* can read message header, don't do anything */
		return;
	if (unread == 0)
	{
		zb->size = 0;
		zb->pos = 0;
		return;
	}
	memmove(zb->buf, zb->buf + zb->pos, unread);
	zb->size = unread;
	zb->pos = 0;
}

struct ZpqStream
{
	ZStream    *z_stream;		/* underlying compression stream */
	char		tx_msg_h_buf[5];	/* incomplete message header buffer */
	size_t		tx_msg_h_size;
	size_t		tx_msg_h_pos;

	size_t		tx_total;		/* amount of bytes sent to tx_func */
	size_t		tx_total_raw;	/* amount of bytes received by zpq_write */
	size_t		rx_total;		/* amount of bytes read by rx_func */
	size_t		rx_total_raw;	/* amount of bytes returned by zpq_write */

	bool		is_compressing; /* current compression state */
	bool		is_decompressing;	/* current decompression state */

	size_t		rx_msg_bytes_left;	/* number of bytes left to process without
									 * changing the decompression state */
	size_t		tx_msg_bytes_left;	/* number of bytes left to process without
									 * changing the compression state */

	ZpqBuffer	rx;				/* buffer for readahead data read by rx_func */
	ZpqBuffer	tx;				/* buffer for data waiting for send via
								 * tx_func */

	zpq_rx_func rx_func;
	zpq_tx_func tx_func;
	void	   *arg;
};

/*
 * Check if should compress message of msg_type with msg_len.
 * Return true if should, false if should not.
 */
static inline bool
zpq_should_compress(char msg_type, uint32 msg_len)
{
	return msg_type == 'd' || msg_type == 'D';	/* subject to change? */
}

/*
 * Check if message is a CompressedMessage.
 * Return true if it is, otherwise false.
 * */
static inline bool
zpq_is_compressed_message(char msg_type)
{
	return msg_type == ZPQ_COMPRESSED_MSG_TYPE;
}

ZpqStream *
zpq_create(int c_alg_impl, int c_level, int d_alg_impl, zpq_tx_func tx_func, zpq_rx_func rx_func, void *arg, char *rx_data, size_t rx_data_size)
{
	ZpqStream  *zpq = (ZpqStream *) malloc(sizeof(ZpqStream));

	zpq->is_compressing = false;
	zpq->is_decompressing = false;
	zpq->rx_msg_bytes_left = 0;
	zpq->tx_msg_bytes_left = 0;
	zpq->tx_msg_h_size = 0;
	zpq->tx_msg_h_pos = 0;

	zpq->tx_total = 0;
	zpq->tx_total_raw = 0;
	zpq->rx_total = 0;
	zpq->rx_total_raw = 0;

	zpq_buf_init(&zpq->rx);
	zpq_buf_size_advance(&zpq->rx, rx_data_size);
	Assert(rx_data_size < ZPQ_BUFFER_SIZE);
	memcpy(zpq->rx.buf, rx_data, rx_data_size);

	zpq_buf_init(&zpq->tx);

	zpq->rx_func = rx_func;
	zpq->tx_func = tx_func;
	zpq->arg = arg;

	zpq->z_stream = zs_create(c_alg_impl, c_level, d_alg_impl);
	if (zpq->z_stream == NULL)
	{
		free(zpq);
		return NULL;
	}
	return zpq;
}

/* Compress up to src_size bytes from *src into CompressedMessage and write it to the tx buffer.
 * Returns ZS_OK on success, ZS_COMPRESS_ERROR if encountered a compression error. */
static inline ssize_t
zpq_write_compressed_message(ZpqStream * zpq, char const *src, size_t src_size, size_t *src_processed)
{
	size_t		compressed_len;
	ssize_t		rc;
	uint32		size;

	/* check if have enough space */
	if (zpq_buf_left(&zpq->tx) <= 5)
	{
		/* too little space for CompressedMessage, abort */
		*src_processed = 0;
		return ZS_OK;
	}

	compressed_len = 0;
	rc = zs_write(zpq->z_stream, src, src_size, src_processed,
				  zpq_buf_size(&zpq->tx) + 5, zpq_buf_left(&zpq->tx) - 5, &compressed_len);

	if (compressed_len > 0)
	{
		*zpq_buf_size(&zpq->tx) = ZPQ_COMPRESSED_MSG_TYPE;	/* write
															 * CompressedMessage
															 * type */
		size = pg_hton32(compressed_len + 4);

		memcpy(zpq_buf_size(&zpq->tx) + 1, &size, sizeof(uint32));	/* write msg length */
		compressed_len += 5;	/* append header length to compressed data
								 * length */
	}

	zpq->tx_total_raw += *src_processed;
	zpq->tx_total += compressed_len;

	zpq_buf_size_advance(&zpq->tx, compressed_len);
	return rc;
}

/* Copy the data directly from *src to the tx buffer */
static void
zpq_write_uncompressed(ZpqStream * zpq, char const *src, size_t src_size, size_t *src_processed)
{
	src_size = Min(zpq_buf_left(&zpq->tx), src_size);
	memcpy(zpq_buf_size(&zpq->tx), src, src_size);

	zpq->tx_total_raw += src_size;
	zpq->tx_total += src_size;
	zpq_buf_size_advance(&zpq->tx, src_size);
	*src_processed = src_size;
}

/* Determine if should compress the next message and
 * change the current compression state */
static ssize_t
zpq_toggle_compression(ZpqStream * zpq, char msg_type, uint32 msg_len)
{
	if (zpq_should_compress(msg_type, msg_len))
	{
		zpq->is_compressing = true;
	}
	else if (zpq->is_compressing)
	{
		/*
		 * Won't compress the next message, should now finish the compression.
		 * Make sure there is no buffered data left in underlying compression
		 * stream
		 */
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
 * Returns number of written raw bytes or error code.
 * In the last case number of bytes written is stored in *processed.
 */
static ssize_t
zpq_write_internal(ZpqStream * zpq, void const *src, size_t src_size, size_t *processed)
{
	size_t		src_pos = 0;
	ssize_t		rc;

	do
	{
		/*
		 * to reduce the socket write calls count and increase average payload
		 * length, do not call tx_func until absolutely necessary.
		 * should_send_now is true if there is no buffered data left in
		 * compression buffers and one of the following is true: 1. Processed
		 * all of the *src data 2. Have some *src data (<5 bytes) left
		 * unprocessed but can't process it because full message header is
		 * needed to determine tx_msg_bytes_left size.
		 */
		bool		should_send_now = (src_size - src_pos == 0 || (src_size - src_pos < 5 && zpq->tx_msg_bytes_left == 0)) && !zs_buffered_tx(zpq->z_stream);

		/*
		 * call the tx_func if should_send_now or too few space left in TX
		 * buffer (<16 bytes)
		 */
		while (zpq_buf_unread(&zpq->tx) && (should_send_now || zpq_buf_left(&zpq->tx) < 16))
		{
			rc = zpq->tx_func(zpq->arg, zpq_buf_pos(&zpq->tx), zpq_buf_unread(&zpq->tx));
			if (rc > 0)
			{
				zpq_buf_pos_advance(&zpq->tx, rc);
			}
			else
			{
				*processed = src_pos;
				return rc;
			}
		}
		zpq_buf_reuse(&zpq->tx);
		if (!zpq_buffered_tx(zpq) && src_size - src_pos == 0)
		{
			/* don't have anything to process, do not proceed further */
			break;
		}

		/*
		 * try to read ahead the next message types and increase
		 * tx_msg_bytes_left, if possible
		 */
		while (zpq->tx_msg_bytes_left > 0 && src_size - src_pos >= zpq->tx_msg_bytes_left + 5)
		{
			char		msg_type = *((char *) src + src_pos + zpq->tx_msg_bytes_left);
			uint32		msg_len;

			memcpy(&msg_len, (char *) src + src_pos + zpq->tx_msg_bytes_left + 1, 4);
			msg_len = pg_ntoh32(msg_len);
			if (zpq_should_compress(msg_type, msg_len) != zpq->is_compressing)
			{
				/*
				 * cannot proceed further, encountered compression toggle
				 * point
				 */
				break;
			}
			zpq->tx_msg_bytes_left += msg_len + 1;
		}

		/*
		 * Write CompressedMessage if currently is compressing or have some
		 * buffered data left in underlying compression stream
		 */
		if (zs_buffered_tx(zpq->z_stream) || (zpq->is_compressing && zpq->tx_msg_bytes_left > 0))
		{
			size_t		buf_processed = 0;
			size_t		to_compress = Min(zpq->tx_msg_bytes_left, src_size - src_pos);

			rc = zpq_write_compressed_message(zpq, (char *) src + src_pos, to_compress, &buf_processed);
			src_pos += buf_processed;
			zpq->tx_msg_bytes_left -= buf_processed;

			if (rc != ZS_OK)
			{
				*processed = src_pos;
				return rc;
			}
		}

		/*
		 * If not going to compress the data from *src, just write it
		 * uncompressed.
		 */
		else if (zpq->tx_msg_bytes_left > 0)
		{						/* determine next message type */
			size_t		copy_len = Min(src_size - src_pos, zpq->tx_msg_bytes_left);
			size_t		copy_processed = 0;

			zpq_write_uncompressed(zpq, (char *) src + src_pos, copy_len, &copy_processed);
			src_pos += copy_processed;
			zpq->tx_msg_bytes_left -= copy_processed;
		}

		/*
		 * Reached the compression toggle point, fetch next message header to
		 * determine compression state.
		 */
		else
		{
			char		msg_type;
			uint32		msg_len;

			if (src_size - src_pos < 5)
			{
				/*
				 * must return here because we can't continue without full
				 * message header
				 */
				return src_pos;
			}

			msg_type = *((char *) src + src_pos);
			memcpy(&msg_len, (char *) src + src_pos + 1, 4);
			msg_len = pg_ntoh32(msg_len);
			rc = zpq_toggle_compression(zpq, msg_type, msg_len);
			if (rc)
			{
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
zpq_write(ZpqStream * zpq, void const *src, size_t src_size, size_t *src_processed)
{
	size_t		src_pos = 0;
	ssize_t		rc;

	do
	{
		/* reset the incomplete message header buffer if it has been processed */
		if (zpq->tx_msg_h_pos == zpq->tx_msg_h_size)
		{
			Assert(zpq->tx_msg_h_size == 0 || zpq->tx_msg_h_size == 5);
			zpq->tx_msg_h_pos = zpq->tx_msg_h_size = 0;
		}

		if ((zpq->tx_msg_bytes_left > 0 || src_size - src_pos >= 5 || zpq_buffered_tx(zpq)) && zpq->tx_msg_h_size == 0)
		{
			rc = zpq_write_internal(zpq, (char *) src + src_pos, src_size - src_pos, src_processed);
			if (rc > 0)
			{
				rc += src_pos;
			}
			else
			{
				*src_processed += src_pos;
			}
			return rc;
		}

		if (zpq->tx_msg_h_size < 5)
		{
			/* read more data into incomplete header buffer */
			size_t		to_copy = Min(5 - zpq->tx_msg_h_size, src_size - src_pos);

			memcpy((char *) zpq->tx_msg_h_buf + zpq->tx_msg_h_size, (char *) src + src_pos, to_copy);
			zpq->tx_msg_h_size += to_copy;
			src_pos += to_copy;
			if (zpq->tx_msg_h_size < 5)
			{
				/* message header is still incomplete, can't proceed further */
				Assert(src_size - src_pos == 0);
				return src_pos;
			}
		}

		Assert(zpq->tx_msg_h_size == 5);
		rc = zpq_write_internal(zpq, (char *) zpq->tx_msg_h_buf + zpq->tx_msg_h_pos, zpq->tx_msg_h_size - zpq->tx_msg_h_pos,
								src_processed);
		if (rc > 0)
		{
			zpq->tx_msg_h_pos += rc;
		}
		else
		{
			zpq->tx_msg_h_pos += *src_processed;
			*src_processed = src_pos;
			return rc;
		}
	} while (src_pos < src_size || zpq_buffered_rx(zpq));
	return src_pos;
}


/* Decompress bytes from RX buffer and write up to dst_len of uncompressed data to *dst.
 * Returns:
 * ZS_OK on success,
 * ZS_STREAM_END if reached end of compressed chunk
 * ZS_DECOMPRESS_ERROR if encountered a decompression error */
static inline ssize_t
zpq_read_compressed_message(ZpqStream * zpq, char *dst, size_t dst_len, size_t *dst_processed)
{
	size_t		rx_processed = 0;
	ssize_t		rc;
	size_t		read_len = Min(zpq->rx_msg_bytes_left, zpq_buf_unread(&zpq->rx));

	rc = zs_read(zpq->z_stream, zpq_buf_pos(&zpq->rx), read_len, &rx_processed,
				 dst, dst_len, dst_processed);

	zpq_buf_pos_advance(&zpq->rx, rx_processed);
	zpq->rx_total_raw += *dst_processed;
	zpq->rx_msg_bytes_left -= rx_processed;
	return rc;
}

/* Copy up to dst_len bytes from rx buffer to *dst.
 * Returns amount of bytes copied. */
static inline size_t
zpq_read_uncompressed(ZpqStream * zpq, char *dst, size_t dst_len)
{
	Assert(zpq_buf_unread(&zpq->rx) > 0);
	size_t		copy_len = Min(zpq->rx_msg_bytes_left, Min(zpq_buf_unread(&zpq->rx), dst_len));

	memcpy(dst, zpq_buf_pos(&zpq->rx), copy_len);

	zpq_buf_pos_advance(&zpq->rx, copy_len);
	zpq->rx_total_raw += copy_len;
	zpq->rx_msg_bytes_left -= copy_len;
	return copy_len;
}

/* Determine if should decompress the next message and
 * change the current decompression state */
static inline void
zpq_toggle_decompression(ZpqStream * zpq)
{
	uint32		msg_len;
	char		msg_type = *zpq_buf_pos(&zpq->rx);

	zpq->is_decompressing = zpq_is_compressed_message(msg_type);

	memcpy(&msg_len, zpq_buf_pos(&zpq->rx) + 1, 4);
	zpq->rx_msg_bytes_left = pg_ntoh32(msg_len) + 1;

	if (zpq->is_decompressing)
	{
		/* compressed message header is no longer needed, just skip it */
		zpq_buf_pos_advance(&zpq->rx, 5);
		zpq->rx_msg_bytes_left -= 5;
	}
}

ssize_t
zpq_read(ZpqStream * zpq, void *dst, size_t dst_size)
{
	size_t		dst_pos = 0;
	size_t		dst_processed = 0;
	ssize_t		rc;

	/* Read until some data fetched */
	while (dst_pos == 0)
	{
		zpq_buf_reuse(&zpq->rx);

		if (!zpq_buffered_rx(zpq))
		{
			rc = zpq->rx_func(zpq->arg, zpq_buf_size(&zpq->rx), zpq_buf_left(&zpq->rx));
			if (rc > 0)			/* read fetches some data */
			{
				zpq->rx_total += rc;
				zpq_buf_size_advance(&zpq->rx, rc);
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
		while (zpq->rx_msg_bytes_left > 0 && (zpq_buf_unread(&zpq->rx) >= zpq->rx_msg_bytes_left + 5))
		{
			char		msg_type;

			msg_type = *(zpq_buf_pos(&zpq->rx) + zpq->rx_msg_bytes_left);
			if (zpq->is_decompressing || zpq_is_compressed_message(msg_type))
			{
				/*
				 * cannot proceed further, encountered compression toggle
				 * point
				 */
				break;
			}
			uint32		msg_len;

			memcpy(&msg_len, zpq_buf_pos(&zpq->rx) + zpq->rx_msg_bytes_left + 1, 4);
			zpq->rx_msg_bytes_left += pg_ntoh32(msg_len) + 1;
		}


		if (zpq->rx_msg_bytes_left > 0 || zs_buffered_rx(zpq->z_stream))
		{
			dst_processed = 0;
			if (zpq->is_decompressing || zs_buffered_rx(zpq->z_stream))
			{
				rc = zpq_read_compressed_message(zpq, dst, dst_size - dst_pos, &dst_processed);
				dst_pos += dst_processed;
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
				dst_pos += zpq_read_uncompressed(zpq, dst, dst_size - dst_pos);
		}
		else if (zpq_buf_unread(&zpq->rx) >= 5)
			zpq_toggle_decompression(zpq);
	}
	return dst_pos;
}

bool
zpq_buffered_rx(ZpqStream * zpq)
{
	return zpq ? zpq_buf_unread(&zpq->rx) >= 5 || (zpq_buf_unread(&zpq->rx) > 0 && zpq->rx_msg_bytes_left > 0) || zs_buffered_rx(zpq->z_stream) : 0;
}

bool
zpq_buffered_tx(ZpqStream * zpq)
{
	return zpq ? zpq->tx_msg_h_size == 5 || (zpq->tx_msg_h_size > 0 && zpq->tx_msg_bytes_left > 0) || zpq_buf_unread(&zpq->tx) > 0 ||
		zs_buffered_tx(zpq->z_stream) : 0;
}

void
zpq_free(ZpqStream * zpq)
{
	if (zpq)
	{
		if (zpq->z_stream)
		{
			zs_free(zpq->z_stream);
		}
		free(zpq);
	}
}

char const *
zpq_compress_error(ZpqStream * zpq)
{
	return zs_compress_error(zpq->z_stream);
}

char const *
zpq_decompress_error(ZpqStream * zpq)
{
	return zs_decompress_error(zpq->z_stream);
}

char const *
zpq_compress_algorithm_name(ZpqStream * zpq)
{
	return zs_compress_algorithm_name(zpq->z_stream);
}
