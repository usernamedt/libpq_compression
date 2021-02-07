#include "postgres_fe.h"
#include "common/zpq_stream.h"
#include "c.h"
#include "pg_config.h"
#include "port/pg_bswap.h"
#include "common/z_stream.h"

/* Check if should compress provided msg_type.
 * Return true if should, false if should not.
 */
#define zpq_should_compress(msg_type, msg_len) (msg_len > 60)

#define zpq_should_decompress(msg_type) (msg_type == 'm')


#define ZPQ_BUFFER_SIZE       81920 /* We have to flush stream after each
									 * protocol command and command is mostly
									 * limited by record length, which in turn
									 * is usually less than page size (except
									 * TOAST) */

struct ZpqStream
{
	ZStream    *zs;
	char		tx_msg_h_buf[5];
	size_t		tx_msg_h_size;

	size_t		tx_total;
	size_t		tx_total_raw;
	size_t		rx_total;
	size_t		rx_total_raw;

	bool		is_compressing;
	bool		is_decompressing;

	size_t		rx_msg_bytes_left;
	size_t		tx_msg_bytes_left;

	char		readahead_buf[ZPQ_BUFFER_SIZE];
	size_t		readahead_pos;
	size_t		readahead_size;

	char		tx_buf[ZPQ_BUFFER_SIZE];
	size_t		tx_pos;
	size_t		tx_size;

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

	zc->tx_total = 0;
	zc->tx_total_raw = 0;
	zc->rx_total = 0;
	zc->rx_total_raw = 0;

	zc->readahead_pos = 0;
	zc->readahead_size = rx_data_size;
	Assert(rx_data_size < ZPQ_BUFFER_SIZE);
	memcpy(zc->readahead_buf, rx_data, rx_data_size);

	zc->tx_pos = 0;
	zc->tx_size = 0;

	zc->rx_func = rx_func;
	zc->tx_func = tx_func;
	zc->arg = arg;

	zc->zs = zs_create(c_alg_impl, c_level, d_alg_impl);
	if (zc->zs == NULL)
	{
		free(zc);
		return NULL;
	}
	return zc;
}

static ssize_t
zpq_write_compressed(ZpqStream * zc, char const *src, size_t src_size, size_t *src_processed)
{
	/* check if have some space */
	if (ZPQ_BUFFER_SIZE - zc->tx_size <= 5)
	{
		/* too little space for compressed message, just return */
		*src_processed = 0;
		return ZS_OK;
	}

	size_t		compressed_len;
	ssize_t		rc = zs_write(zc->zs, src, src_size, src_processed,
							  (char *) zc->tx_buf + zc->tx_size + 5, ZPQ_BUFFER_SIZE - zc->tx_size - 5, &compressed_len);

	if (compressed_len > 0)
	{
		*((char *) zc->tx_buf + zc->tx_size) = 'm'; /* write compressed
													 * message type */
		uint32		size = pg_hton32(compressed_len + 4);

		memcpy((char *) zc->tx_buf + zc->tx_size + 1, &size, sizeof(uint32));	/* write compressed
																				 * message length */
		compressed_len += 5;
	}

	zc->tx_total_raw += *src_processed;
	zc->tx_total += compressed_len;

	zc->tx_size += compressed_len;
	return rc;
}

static void
zpq_write_raw(ZpqStream * zc, char const *src, size_t src_size, size_t *src_processed)
{
	src_size = Min(ZPQ_BUFFER_SIZE - zc->tx_size, src_size);

	memcpy((char *) zc->tx_buf + zc->tx_size, src, src_size);

	zc->tx_total_raw += src_size;
	zc->tx_total += src_size;
	zc->tx_size += src_size;
	*src_processed = src_size;
}

ssize_t
zpq_write(ZpqStream * zq, void const *buf, size_t size, size_t *processed)
{
	size_t		buf_pos = 0;
	ssize_t		rc;

	do
	{
		/*
		 * send all pending data if no more free space in ZPQ buffer (< 16
		 * bytes) or don't have any non-processed data left
		 */
		while (zq->tx_pos < zq->tx_size &&
			   ((size - buf_pos == 0 && !zs_buffered_tx(zq->zs)) || ZPQ_BUFFER_SIZE - zq->tx_size < 16))
		{
			rc = zq->tx_func(zq->arg, (char *) zq->tx_buf + zq->tx_pos, zq->tx_size - zq->tx_pos);

			if (rc > 0)
			{
				zq->tx_pos += rc;
			}
			else
			{
				*processed = buf_pos;
				return rc;
			}
		}
		if (zq->tx_pos == zq->tx_size)
		{
			zq->tx_pos = zq->tx_size = 0;	/* Reset pointer to the beginning
											 * of buffer */
		}

		if (!zpq_buffered_tx(zq) && size - buf_pos == 0)
		{
			continue;			/* don't have anything to process, do not
								 * proceed further */
		}

		/*
		 * try to read ahead the next message types and increase
		 * tx_msg_bytes_left, if possible
		 */
		while (zq->tx_msg_bytes_left > 0 && size - buf_pos >= zq->tx_msg_bytes_left + 5 - zq->tx_msg_h_size)
		{
			char		msg_type = *((char *) buf + buf_pos + zq->tx_msg_bytes_left - zq->tx_msg_h_size);
			uint32		msg_len;

			memcpy(&msg_len, (char *) buf + buf_pos + zq->tx_msg_bytes_left - zq->tx_msg_h_size + 1, 4);
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

		if (zs_buffered_tx(zq->zs) || (zq->is_compressing && zq->tx_msg_bytes_left > 0))
		{
			size_t		buf_processed = 0;

			if (zq->tx_msg_h_size > 0)
			{
				rc = zpq_write_compressed(zq, (char *) zq->tx_msg_h_buf + 5 - zq->tx_msg_h_size, zq->tx_msg_h_size,
										  &buf_processed);
				zq->tx_msg_h_size -= buf_processed;
			}
			else
			{
				size_t		to_compress = Min(zq->tx_msg_bytes_left, size - buf_pos);

				rc = zpq_write_compressed(zq, (char *) buf + buf_pos, to_compress, &buf_processed);
				buf_pos += buf_processed;
			}
			zq->tx_msg_bytes_left -= buf_processed;

			if (rc != ZS_OK)
			{
				*processed = buf_pos;
				return rc;
			}
		}
		else if (zq->tx_msg_bytes_left == 0)
		{						/* determine next message type */
			/*
			 * try to get next msg type, then set is_compressing and
			 * tx_msg_bytes_left
			 */
			if (zq->tx_msg_h_size == 5)
			{					/* read msg type and length if possible */
				char		msg_type = zq->tx_msg_h_buf[0];
				uint32		msg_len;

				memcpy(&msg_len, (char *) zq->tx_msg_h_buf + 1, 4);
				msg_len = pg_ntoh32(msg_len);

				if (zpq_should_compress(msg_type, msg_len))
				{
					zq->is_compressing = true;
				}
				else if (zq->is_compressing)
				{
					while (zs_buffered_tx(zq->zs))	/* make sure there is no
													 * buffered data left in
													 * compressor */
					{
						size_t		flush_processed = 0;
						ssize_t		flush_rc = zpq_write_compressed(zq, NULL, 0, &flush_processed);

						if (flush_rc != ZS_OK)
						{
							return flush_rc;
						}
					}
					zq->is_compressing = false;
				}
				zq->tx_msg_bytes_left = msg_len + 1;
			}
			else
			{
				/* copy available tx data to the message header buffer */
				size_t		to_copy = Min(5 - zq->tx_msg_h_size, size - buf_pos);

				memcpy((char *) zq->tx_msg_h_buf + zq->tx_msg_h_size, (char *) buf + buf_pos, to_copy);

				zq->tx_msg_h_size += to_copy;
				buf_pos += to_copy;
			}
		}
		else
		{
			size_t		copy_len = zq->tx_msg_bytes_left;
			size_t		copy_processed = 0;

			if (zq->tx_msg_h_size > 0)
			{
				copy_len = Min(zq->tx_msg_h_size, copy_len);
				zpq_write_raw(zq, (char *) zq->tx_msg_h_buf + 5 - zq->tx_msg_h_size, copy_len, &copy_processed);
				zq->tx_msg_h_size -= copy_processed;
			}
			else
			{
				copy_len = Min(size - buf_pos, copy_len);
				zpq_write_raw(zq, (char *) buf + buf_pos, copy_len, &copy_processed);
				buf_pos += copy_processed;
			}
			zq->tx_msg_bytes_left -= copy_processed;
		}

		/*
		 * repeat sending while there is some data in input or internal
		 * compression buffer
		 */
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

	while (buf_pos == 0)
	{							/* Read until some data fetched */
		if (zc->readahead_pos > 0)
		{
			if (zc->readahead_size > zc->readahead_pos)
			{
				/* still some unread data, left-justify it in the buffer */
				memmove(zc->readahead_buf, zc->readahead_buf + zc->readahead_pos,
						zc->readahead_size - zc->readahead_pos);
				zc->readahead_size -= zc->readahead_pos;
				zc->readahead_pos = 0;
			}
			else
				zc->readahead_pos = zc->readahead_size = 0; /* Reset rx buffer */
		}

		if (!zpq_buffered_rx(zc))
		{
			rc = zc->rx_func(zc->arg, (char *) zc->readahead_buf + zc->readahead_size, ZPQ_BUFFER_SIZE - zc->readahead_size);
			if (rc > 0)			/* read fetches some data */
			{
				zc->rx_total += rc;
				zc->readahead_size += rc;
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
		while (zc->rx_msg_bytes_left > 0 && (zc->readahead_size - zc->readahead_pos >= zc->rx_msg_bytes_left + 5))
		{
			char		msg_type;

			msg_type = *((char *) zc->readahead_buf + zc->readahead_pos + zc->rx_msg_bytes_left);
			if (zc->is_decompressing || zpq_should_decompress(msg_type))
			{
				/*
				 * cannot proceed further, encountered compression toggle
				 * point
				 */
				break;
			}
			uint32		msg_len;

			memcpy(&msg_len, (char *) zc->readahead_buf + zc->readahead_pos + zc->rx_msg_bytes_left + 1, 4);
			zc->rx_msg_bytes_left += pg_ntoh32(msg_len) + 1;
		}


		if (zc->rx_msg_bytes_left > 0 || zs_buffered_rx(zc->zs))
		{
			if (zc->is_decompressing || zs_buffered_rx(zc->zs))
			{
				Assert(zc->readahead_pos <= zc->readahead_size);
				rx_processed = 0;
				buf_processed = 0;

				size_t		read_count = Min(zc->rx_msg_bytes_left, zc->readahead_size - zc->readahead_pos);

				rc = zs_read(zc->zs, (char *) zc->readahead_buf + zc->readahead_pos, read_count, &rx_processed,
							 buf, size - buf_pos, &buf_processed);

				zc->readahead_pos += rx_processed;
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
				Assert(zc->readahead_pos < zc->readahead_size);
				size_t		copy_len = Min(zc->rx_msg_bytes_left, Min(zc->readahead_size - zc->readahead_pos, size - buf_pos));

				memcpy(buf, (char *) zc->readahead_buf + zc->readahead_pos, copy_len);

				zc->readahead_pos += copy_len;
				buf_pos += copy_len;
				zc->rx_total_raw += copy_len;
				zc->rx_msg_bytes_left -= copy_len;
			}
		}
		else if (zc->readahead_size - zc->readahead_pos >= 5)
		{						/* determine next message type */
			uint32		msg_len;

			/* read msg type and length if possible */
			char		msg_type = zc->readahead_buf[zc->readahead_pos];

			zc->is_decompressing = zpq_should_decompress(msg_type);

			memcpy(&msg_len, zc->readahead_buf + zc->readahead_pos + 1, 4);
			zc->rx_msg_bytes_left = pg_ntoh32(msg_len) + 1;

			if (zc->is_decompressing)
			{
				/* compressed message header is no longer needed, just skip it */
				zc->readahead_pos += 5;
				zc->rx_msg_bytes_left -= 5;
			}
		}
	}
	return buf_pos;
}

size_t
zpq_buffered_rx(ZpqStream * zc)
{
	return zc ? zc->readahead_size - zc->readahead_pos >= 5 || (zc->readahead_size - zc->readahead_pos > 0 && zc->rx_msg_bytes_left > 0) ||
		zs_buffered_rx(zc->zs) : 0;
}

size_t
zpq_buffered_tx(ZpqStream * zc)
{
	return zc ? zc->tx_msg_h_size == 5 || (zc->tx_msg_h_size > 0 && zc->tx_msg_bytes_left > 0) || zc->tx_size - zc->tx_pos > 0 ||
		zs_buffered_tx(zc->zs) : 0;
}

void
zpq_free(ZpqStream * zc)
{
	if (zc)
	{
		if (zc->zs)
		{
			zs_free(zc->zs);
		}
		free(zc);
	}
}

char const *
zpq_compress_error(ZpqStream * zc)
{
	return zs_compress_error(zc->zs);
}

char const *
zpq_decompress_error(ZpqStream * zc)
{
	return zs_decompress_error(zc->zs);
}

char const *
zpq_compress_algorithm_name(ZpqStream * zc)
{
	return zs_compress_algorithm_name(zc->zs);
}
