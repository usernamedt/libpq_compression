#include "postgres_fe.h"
#include "common/zpq_stream.h"
#include "c.h"
#include "pg_config.h"
#include "port/pg_bswap.h"

/* Check if should compress provided msg_type.
 * Return true if should, false if should not.
 */
#define zc_should_compress(msg_type, msg_len) (msg_len > 200)

#define zc_should_decompress(msg_type) (msg_type == 'm')

/*
 * Functions implementing streaming compression algorithm
 */
typedef struct
{
	/*
	 * Name of compression algorithm.
	 */
	char const *(*name) (void);

	/*
	 * Create new compression stream.
	 * level: compression level
	 */
	void	   *(*create_compressor) (int level);

	/*
	 * Create new decompression stream.
	 */
	void	   *(*create_decompressor) ();

	/*
	 * Decompress up to "src_size" compressed bytes from *src and write up to
	 * "dst_size" raw (decompressed) bytes to *dst. Number of decompressed
	 * bytes written to *dst is stored in *dst_processed. Number of compressed
	 * bytes read from *src is stored in *src_processed.
	 *
	 * Return codes: ZPQ_OK if no errors were encountered during decompression
	 * attempt. This return code does not guarantee that *src_processed > 0 or
	 * *dst_processed > 0.
	 *
	 * ZPQ_DATA_PENDING means that there might be some data left within
	 * decompressor internal buffers.
	 *
	 * ZPQ_STREAM_END if encountered end of compressed data stream.
	 *
	 * ZPQ_DECOMPRESS_ERROR if encountered an error during decompression
	 * attempt.
	 */
	ssize_t		(*decompress) (void *ds, void const *src, size_t src_size, size_t *src_processed, void *dst, size_t dst_size, size_t *dst_processed);

	/*
	 * Compress up to "src_size" raw (non-compressed) bytes from *src and
	 * write up to "dst_size" compressed bytes to *dst. Number of compressed
	 * bytes written to *dst is stored in *dst_processed. Number of
	 * non-compressed bytes read from *src is stored in *src_processed.
	 *
	 * Return codes: ZPQ_OK if no errors were encountered during compression
	 * attempt. This return code does not guarantee that *src_processed > 0 or
	 * *dst_processed > 0.
	 *
	 * ZPQ_DATA_PENDING means that there might be some data left within
	 * compressor internal buffers.
	 *
	 * ZPQ_COMPRESS_ERROR if encountered an error during compression attempt.
	 */
	ssize_t		(*compress) (void *cs, void const *src, size_t src_size, size_t *src_processed, void *dst, size_t dst_size, size_t *dst_processed);

	/*
	 * Free compression stream created by create_compressor function.
	 */
	void		(*free_compressor) (void *cs);

	/*
	 * Free decompression stream created by create_decompressor function.
	 */
	void		(*free_decompressor) (void *ds);

	/*
	 * Get compressor error message.
	 */
	char const *(*compress_error) (void *cs);

	/*
	 * Get decompressor error message.
	 */
	char const *(*decompress_error) (void *ds);

    ssize_t (*flush_compression)(void *cs, void *dst, size_t dst_size, size_t *dst_processed);
}			ZpqAlgorithm;


#define ZPQ_BUFFER_SIZE       81920	/* We have to flush stream after each
									 * protocol command and command is mostly
									 * limited by record length, which in turn
									 * is usually less than page size (except
									 * TOAST)
									 */

struct ZpqStream
{
	ZpqAlgorithm const *c_algorithm;
	void	   *c_stream;

	ZpqAlgorithm const *d_algorithm;
	void	   *d_stream;

	size_t		tx_total;
	size_t		tx_total_raw;
	size_t		rx_total;
	size_t		rx_total_raw;

	bool		rx_not_flushed;
	bool		tx_not_flushed;
};

struct ZpqController {
    ZpqStream      *zs;
    char           tx_msg_h_buf[5];
    size_t         tx_msg_h_size;

    bool           is_compressing;
    bool           is_decompressing;

    size_t         rx_msg_bytes_left;
    size_t         tx_msg_bytes_left;

    char		readahead_buf[ZPQ_BUFFER_SIZE];
    size_t		readahead_pos;
    size_t      readahead_size;

    char		compressed_buf[ZPQ_BUFFER_SIZE];
    size_t		compressed_pos;
    size_t		compressed_size;

    zpq_rx_func rx_func;
    zpq_tx_func tx_func;
    void *arg;
};

#if HAVE_LIBZSTD

#include <stdlib.h>
#include <zstd.h>

/*
 * Maximum allowed back-reference distance, expressed as power of 2.
 * This setting controls max compressor/decompressor window size.
 * More details https://github.com/facebook/zstd/blob/v1.4.7/lib/zstd.h#L536
 */
#define ZSTD_WINDOWLOG_LIMIT 23 /* set max window size to 8MB */

typedef struct ZPQ_ZSTD_CStream
{
	ZSTD_CStream *stream;
	char const *error;			/* error message */
}			ZPQ_ZSTD_CStream;

typedef struct ZPQ_ZSTD_DStream
{
	ZSTD_DStream *stream;
	char const *error;			/* error message */
}			ZPQ_ZSTD_DStream;

static void *
zstd_create_compressor(int level)
{
	ZPQ_ZSTD_CStream *c_stream = (ZPQ_ZSTD_CStream *) malloc(sizeof(ZPQ_ZSTD_CStream));

	c_stream->stream = ZSTD_createCStream();
	ZSTD_initCStream(c_stream->stream, level);
#if ZSTD_VERSION_MAJOR > 1 || ZSTD_VERSION_MINOR > 3
	ZSTD_CCtx_setParameter(c_stream->stream, ZSTD_c_windowLog, ZSTD_WINDOWLOG_LIMIT);
#endif
	c_stream->error = NULL;
	return c_stream;
}

static void *
zstd_create_decompressor()
{
	ZPQ_ZSTD_DStream *d_stream = (ZPQ_ZSTD_DStream *) malloc(sizeof(ZPQ_ZSTD_DStream));

	d_stream->stream = ZSTD_createDStream();
	ZSTD_initDStream(d_stream->stream);
#if ZSTD_VERSION_MAJOR > 1 || ZSTD_VERSION_MINOR > 3
	ZSTD_DCtx_setParameter(d_stream->stream, ZSTD_d_windowLogMax, ZSTD_WINDOWLOG_LIMIT);
#endif
	d_stream->error = NULL;
	return d_stream;
}

static ssize_t
zstd_decompress(void *d_stream, void const *src, size_t src_size, size_t *src_processed, void *dst, size_t dst_size, size_t *dst_processed)
{
	ZPQ_ZSTD_DStream *ds = (ZPQ_ZSTD_DStream *) d_stream;
	ZSTD_inBuffer in;
	ZSTD_outBuffer out;
	size_t		rc;

	in.src = src;
	in.pos = 0;
	in.size = src_size;

	out.dst = dst;
	out.pos = 0;
	out.size = dst_size;

	rc = ZSTD_decompressStream(ds->stream, &out, &in);

	*src_processed = in.pos;
	*dst_processed = out.pos;
	if (ZSTD_isError(rc))
	{
		ds->error = ZSTD_getErrorName(rc);
		return ZPQ_DECOMPRESS_ERROR;
	}

    if (rc == 0) {
        return ZPQ_STREAM_END;
    }

	if (out.pos == out.size)
	{
		/*
		 * if `output.pos == output.size`, there might be some data left
		 * within internal buffers
		 */
		return ZPQ_DATA_PENDING;
	}
	return ZPQ_OK;
}

static ssize_t
zstd_compress(void *c_stream, void const *src, size_t src_size, size_t *src_processed, void *dst, size_t dst_size, size_t *dst_processed)
{
	ZPQ_ZSTD_CStream *cs = (ZPQ_ZSTD_CStream *) c_stream;
	ZSTD_inBuffer in;
	ZSTD_outBuffer out;

	in.src = src;
	in.pos = 0;
	in.size = src_size;

	out.dst = dst;
	out.pos = 0;
	out.size = dst_size;

	if (in.pos < src_size)		/* Has something to compress in input buffer */
	{
		size_t		rc = ZSTD_compressStream(cs->stream, &out, &in);

		*dst_processed = out.pos;
		*src_processed = in.pos;
		if (ZSTD_isError(rc))
		{
			cs->error = ZSTD_getErrorName(rc);
			return ZPQ_COMPRESS_ERROR;
		}
	}

	if (in.pos == src_size)		/* All data is compressed: flush internal zstd
								 * buffer */
	{
		size_t		tx_not_flushed = ZSTD_flushStream(cs->stream, &out);

		*dst_processed = out.pos;
		if (tx_not_flushed > 0)
		{
			return ZPQ_DATA_PENDING;
		}
	}

	return ZPQ_OK;
}

static ssize_t zstd_flush(void *c_stream, void *dst, size_t dst_size, size_t *dst_processed) {
    ZPQ_ZSTD_CStream *cs = (ZPQ_ZSTD_CStream *) c_stream;
    ZSTD_outBuffer output;

    output.dst = dst;
    output.pos = 0;
    output.size = dst_size;

    size_t	tx_not_flushed;
    do {
        tx_not_flushed = ZSTD_endStream(cs->stream, &output);
    } while((tx_not_flushed > 0) && (output.pos < output.size));

    *dst_processed = output.pos;

    if (tx_not_flushed > 0) {
        return ZPQ_DATA_PENDING;
    }
    return ZPQ_OK;
}

static void
zstd_free_compressor(void *c_stream)
{
	ZPQ_ZSTD_CStream *cs = (ZPQ_ZSTD_CStream *) c_stream;

	if (cs != NULL)
	{
		ZSTD_freeCStream(cs->stream);
		free(cs);
	}
}

static void
zstd_free_decompressor(void *d_stream)
{
	ZPQ_ZSTD_DStream *ds = (ZPQ_ZSTD_DStream *) d_stream;

	if (ds != NULL)
	{
		ZSTD_freeDStream(ds->stream);
		free(ds);
	}
}

static char const *
zstd_compress_error(void *c_stream)
{
	ZPQ_ZSTD_CStream *cs = (ZPQ_ZSTD_CStream *) c_stream;

	return cs->error;
}

static char const *
zstd_decompress_error(void *d_stream)
{
	ZPQ_ZSTD_DStream *ds = (ZPQ_ZSTD_DStream *) d_stream;

	return ds->error;
}

static char const *
zstd_name(void)
{
	return "zstd";
}

#endif

#if HAVE_LIBZ

#include <stdlib.h>
#include <zlib.h>


static void *
zlib_create_compressor(int level)
{
	int			rc;
	z_stream   *c_stream = (z_stream *) malloc(sizeof(z_stream));

	memset(c_stream, 0, sizeof(*c_stream));
	rc = deflateInit(c_stream, level);
	if (rc != Z_OK)
	{
		free(c_stream);
		return NULL;
	}
	return c_stream;
}

static void *
zlib_create_decompressor()
{
	int			rc;
	z_stream   *d_stream = (z_stream *) malloc(sizeof(z_stream));

	memset(d_stream, 0, sizeof(*d_stream));
	rc = inflateInit(d_stream);
	if (rc != Z_OK)
	{
		free(d_stream);
		return NULL;
	}
	return d_stream;
}

static ssize_t
zlib_decompress(void *d_stream, void const *src, size_t src_size, size_t *src_processed, void *dst, size_t dst_size, size_t *dst_processed)
{
	z_stream   *ds = (z_stream *) d_stream;
	int			rc;

	ds->next_in = (Bytef *) src;
	ds->avail_in = src_size;
	ds->next_out = (Bytef *) dst;
	ds->avail_out = dst_size;

	rc = inflate(ds, Z_SYNC_FLUSH);
	*src_processed = src_size - ds->avail_in;
	*dst_processed = dst_size - ds->avail_out;

	if (rc == Z_STREAM_END)
	{
		return ZPQ_STREAM_END;
	}
	if (rc != Z_OK && rc != Z_BUF_ERROR)
	{
		return ZPQ_DECOMPRESS_ERROR;
	}

	return ZPQ_OK;
}

static ssize_t
zlib_compress(void *c_stream, void const *src, size_t src_size, size_t *src_processed, void *dst, size_t dst_size, size_t *dst_processed)
{
	z_stream   *cs = (z_stream *) c_stream;
	int			rc;
	unsigned	deflate_pending = 0;


	cs->next_out = (Bytef *) dst;
	cs->avail_out = dst_size;
	cs->next_in = (Bytef *) src;
	cs->avail_in = src_size;

	rc = deflate(cs, Z_SYNC_FLUSH);
	Assert(rc == Z_OK);
	*dst_processed = dst_size - cs->avail_out;
	*src_processed = src_size - cs->avail_in;

	deflatePending(cs, &deflate_pending, Z_NULL);	/* check if any data left
													 * in deflate buffer */
	if (deflate_pending > 0)
	{
		return ZPQ_DATA_PENDING;
	}
	return ZPQ_OK;
}

static void
zlib_free_compressor(void *c_stream)
{
	z_stream   *cs = (z_stream *) c_stream;

	if (cs != NULL)
	{
		deflateEnd(cs);
		free(cs);
	}
}

static void
zlib_free_decompressor(void *d_stream)
{
	z_stream   *ds = (z_stream *) d_stream;

	if (ds != NULL)
	{
		inflateEnd(ds);
		free(ds);
	}
}

static char const *
zlib_error(void *stream)
{
	z_stream   *zs = (z_stream *) stream;

	return zs->msg;
}

static char const *
zlib_name(void)
{
	return "zlib";
}

#endif

static char const *
no_compression_name(void)
{
	return NULL;
}

/*
 * Array with all supported compression algorithms.
 */
static ZpqAlgorithm const zpq_algorithms[] =
{
#if HAVE_LIBZSTD
	{zstd_name, zstd_create_compressor, zstd_create_decompressor, zstd_decompress, zstd_compress, zstd_free_compressor, zstd_free_decompressor, zstd_compress_error, zstd_decompress_error, zstd_flush},
#endif
#if HAVE_LIBZ
	{zlib_name, zlib_create_compressor, zlib_create_decompressor, zlib_decompress, zlib_compress, zlib_free_compressor, zlib_free_decompressor, zlib_error, zlib_error, NULL},
#endif
	{no_compression_name}
};

static ssize_t zpq_init_compressor(ZpqStream *zs, int c_alg_impl, int c_level) {
    zs->c_algorithm = &zpq_algorithms[c_alg_impl];
    zs->c_stream = zpq_algorithms[c_alg_impl].create_compressor(c_level);
    if (zs->c_stream == NULL) {
        return -1;
    }
    return 0;
}

static ssize_t zpq_init_decompressor(ZpqStream *zs, int d_alg_impl) {
    zs->d_algorithm = &zpq_algorithms[d_alg_impl];
    zs->d_stream = zpq_algorithms[d_alg_impl].create_decompressor();
    if (zs->d_stream == NULL) {
        return -1;
    }
    return 0;
}

/*
 * Index of used compression algorithm in zpq_algorithms array.
 */
ZpqStream *
zpq_create(int c_alg_impl, int c_level, int d_alg_impl)
{
	ZpqStream  *zs = (ZpqStream *) malloc(sizeof(ZpqStream));
	zs->tx_total = 0;
	zs->tx_total_raw = 0;
	zs->rx_total = 0;
	zs->rx_total_raw = 0;
	zs->tx_not_flushed = false;
	zs->rx_not_flushed = false;

    if (zpq_init_compressor(zs, c_alg_impl, c_level) || zpq_init_decompressor(zs, d_alg_impl)) {
        free(zs);
        return NULL;
    }

    return zs;
}

ssize_t
zpq_read(ZpqStream * zs, void const *src, size_t src_size, size_t *src_processed, void *dst, size_t dst_size, size_t *dst_processed)
{
    *src_processed = 0;
    *dst_processed = 0;

    ssize_t		rc = zs->d_algorithm->decompress(zs->d_stream,
                                              src, src_size, src_processed,
                                              dst, dst_size, dst_processed);

    zs->rx_total += *src_processed;
    zs->rx_total_raw += *dst_processed;

    zs->rx_not_flushed = false;
    if (rc == ZPQ_DATA_PENDING)
    {
        zs->rx_not_flushed = true;
        return ZPQ_OK;
    }
    if (rc != ZPQ_OK)
    {
        return rc;
    }

    return ZPQ_OK;
}

ssize_t
zpq_write(ZpqStream * zs, void const *buf, size_t size, size_t *processed, void *dst, size_t dst_size, size_t *dst_processed)
{
    *processed = 0;
    *dst_processed = 0;

    ssize_t rc = zs->c_algorithm->compress(zs->c_stream,
                                           buf, size, processed,
                                           dst, dst_size, dst_processed);

    zs->tx_total_raw += *processed;
    zs->tx_total += *dst_processed;

    zs->tx_not_flushed = false;
    if (rc == ZPQ_DATA_PENDING)
    {
        zs->tx_not_flushed = true;
        return ZPQ_OK;
    }
    if (rc != ZPQ_OK)
    {
        return ZPQ_COMPRESS_ERROR;
    }

    return ZPQ_OK;
}

void
zpq_free(ZpqStream * zs)
{
	if (zs)
	{
		if (zs->c_stream)
		{
			zs->c_algorithm->free_compressor(zs->c_stream);
		}
		if (zs->d_stream)
		{
			zs->d_algorithm->free_decompressor(zs->d_stream);
		}
		free(zs);
	}
}

char const *
zpq_compress_error(ZpqStream * zs)
{
	return zs->c_algorithm->compress_error(zs->c_stream);
}

char const *
zpq_decompress_error(ZpqStream * zs)
{
	return zs->d_algorithm->decompress_error(zs->d_stream);
}

size_t
zpq_buffered_rx(ZpqStream * zs)
{
	return zs ? zs->rx_not_flushed : 0;
}

size_t
zpq_buffered_tx(ZpqStream * zs)
{
	return zs ? zs->tx_not_flushed : 0;
}

/*
 * Get list of the supported algorithms.
 */
char	  **
zpq_get_supported_algorithms(void)
{
	size_t		n_algorithms = sizeof(zpq_algorithms) / sizeof(*zpq_algorithms);
	char	  **algorithm_names = malloc(n_algorithms * sizeof(char *));

	for (size_t i = 0; i < n_algorithms; i++)
	{
		algorithm_names[i] = (char *) zpq_algorithms[i].name();
	}

	return algorithm_names;
}

char const *
zpq_compress_algorithm_name(ZpqStream * zs)
{
	return zs ? zs->c_algorithm->name() : NULL;
}

char const *
zpq_decompress_algorithm_name(ZpqStream * zs)
{
	return zs ? zs->d_algorithm->name() : NULL;
}

ZpqController *
zpq_c_create(int c_alg_impl, int c_level, int d_alg_impl, zpq_tx_func tx_func, zpq_rx_func rx_func, void *arg, char *rx_data, size_t rx_data_size) {
    ZpqController *zc = (ZpqController *) malloc(sizeof(ZpqController));

    zc->is_compressing = false;
    zc->is_decompressing = false;
    zc->rx_msg_bytes_left = 0;
    zc->tx_msg_bytes_left = 0;
    zc->tx_msg_h_size = 0;

    zc->readahead_pos = 0;
    zc->readahead_size = rx_data_size;
    Assert(rx_data_size < ZPQ_BUFFER_SIZE);
    memcpy(zc->readahead_buf, rx_data, rx_data_size);

    zc->compressed_pos = 0;
    zc->compressed_size = 0;

    zc->rx_func = rx_func;
    zc->tx_func = tx_func;
    zc->arg = arg;

    zc->zs = zpq_create(c_alg_impl, c_level, d_alg_impl);
    if (zc->zs == NULL){
        free(zc);
        return NULL;
    }
    return zc;
}

static ssize_t zpq_c_compress(ZpqController * zc, char const *src, size_t src_size, size_t *src_processed){
    /* check if have some space */
    if (ZPQ_BUFFER_SIZE - zc->compressed_size <= 5) {
        /* too little space for compressed message, just return */
        *src_processed = 0;
        return ZPQ_OK;
    }

    size_t compressed_len;
    ssize_t rc = zpq_write(zc->zs, src, src_size, src_processed,
                           (char *) zc->compressed_buf + zc->compressed_size + 5, ZPQ_BUFFER_SIZE - zc->compressed_size - 5, &compressed_len);

    if (compressed_len > 0) {
        *((char *) zc->compressed_buf + zc->compressed_size) = 'm'; /* write compressed message type */
        uint32 size = pg_hton32(compressed_len + 4);
        memcpy((char *) zc->compressed_buf + zc->compressed_size + 1, &size, sizeof(uint32)); /* write compressed message length */
        compressed_len += 5;
    }

    zc->compressed_size += compressed_len;
    return rc;
}

ssize_t zpq_c_write(ZpqController * zc, void const *buf, size_t size, size_t *processed) {
    size_t		buf_pos = 0;
    ssize_t		rc;

    do
    {
        /* send all pending data */
        while (!(ZPQ_BUFFER_SIZE - zc->compressed_size > 10 && (size - buf_pos > 0 || zpq_buffered_tx(zc->zs))) && zc->compressed_pos < zc->compressed_size)
        {
            rc = zc->tx_func(zc->arg, (char *) zc->compressed_buf + zc->compressed_pos, zc->compressed_size - zc->compressed_pos);

            if (rc > 0)
            {
                zc->compressed_pos += rc;
            }
            else
            {
                *processed = buf_pos;
                return rc;
            }
        }
        zc->compressed_pos = zc->compressed_size = 0; /* Reset pointer to the beginning of buffer */

        if (!zpq_c_buffered_tx(zc) && size - buf_pos == 0) {
            continue; /* don't have anything to process, skip */
        }

        /* try to prefetch the next message types and increase tx_msg_bytes_left, if possible */
        while ((zc->tx_msg_bytes_left > 0) && ((size - buf_pos) >= (zc->tx_msg_bytes_left + 5 - zc->tx_msg_h_size))) {
            char msg_type;
            msg_type = *((char*)buf + buf_pos + zc->tx_msg_bytes_left - zc->tx_msg_h_size);
            uint32 msg_len;
            memcpy(&msg_len, (char*)buf + buf_pos + zc->tx_msg_bytes_left - zc->tx_msg_h_size + 1, 4);
            msg_len = pg_ntoh32(msg_len);
            if (zc_should_compress(msg_type, msg_len) != zc->is_compressing) {
                break; // cannot proceed further
            }
            zc->tx_msg_bytes_left += msg_len + 1;
        }

        if (zpq_buffered_tx(zc->zs) || (zc->is_compressing && zc->tx_msg_bytes_left > 0)) {
            size_t buf_processed = 0;
            ssize_t to_compress;

            if (zc->tx_msg_h_size > 0) {
                rc = zpq_c_compress(zc, (char *) zc->tx_msg_h_buf + 5 - zc->tx_msg_h_size, zc->tx_msg_h_size, &buf_processed);
                zc->tx_msg_h_size -= buf_processed;
            } else {
                if (zc->tx_msg_bytes_left < (size - buf_pos)) {
                    to_compress = zc->tx_msg_bytes_left;
                } else {
                    to_compress = size - buf_pos;
                }
                rc = zpq_c_compress(zc,(char *) buf + buf_pos, to_compress, &buf_processed);
                buf_pos += buf_processed;
            }
            zc->tx_msg_bytes_left -= buf_processed;

            if (rc != ZPQ_OK)
            {
                *processed = buf_pos;
                return rc;
            }
        } else if (zc->tx_msg_bytes_left == 0) { /* determine next message type */
            /* try to get next msg type, then set is_compressing and tx_msg_bytes_left */
            if (zc->tx_msg_h_size == 5) { /* read msg type and length if possible */
                char msg_type;
                msg_type = *((char*)zc->tx_msg_h_buf);
                uint32 msg_len;
                memcpy(&msg_len, (char*)zc->tx_msg_h_buf + 1, 4);
                msg_len = pg_ntoh32(msg_len);

                if (zc_should_compress(msg_type, msg_len)) {
                    zc->is_compressing = true;
                } else {
                    // random message type for test (forbid compressing non-d messages)
                    if (zc->is_compressing) { /* may return to this multiple times */
                        while (zpq_buffered_tx(zc->zs)) {
                            size_t flush_processed = 0;
                            ssize_t flush_rc = zpq_c_compress(zc, NULL, 0, &flush_processed);
                            if (flush_rc != ZPQ_OK) {
                                return flush_rc;
                            }
                        }
                        zc->is_compressing = false;
                    }
                }
                zc->tx_msg_bytes_left = msg_len + 1;
            } else {
                /* wait for more data to come */
                Assert(zc->tx_msg_h_size < 5);
                size_t avail = size - buf_pos;
                if (avail > (5 - zc->tx_msg_h_size)) {
                    avail = (5 - zc->tx_msg_h_size);
                }
                memcpy((char*)zc->tx_msg_h_buf + zc->tx_msg_h_size, (char*)buf + buf_pos, avail);
                zc->tx_msg_h_size += avail;
                buf_pos += avail;
            }
        } else {
            size_t copy_size = zc->tx_msg_bytes_left;
            ssize_t copy_rc;
            if (zc->tx_msg_h_size > 0)
            {
                if (zc->tx_msg_h_size < copy_size) {
                    copy_size = zc->tx_msg_h_size;
                }
                copy_rc = zc->tx_func(zc->arg, (char *) zc->tx_msg_h_buf + 5 - zc->tx_msg_h_size, copy_size);

                if (copy_rc > 0)
                {
                    zc->tx_msg_h_size -= copy_rc;
                }
                else
                {
                    *processed = buf_pos;
                    return copy_rc;
                }
            } else {
                if ((size - buf_pos) < copy_size) {
                    copy_size = size - buf_pos;
                }
                copy_rc = zc->tx_func(zc->arg, (char*)buf + buf_pos, copy_size);
                if (copy_rc > 0){
                    buf_pos += copy_rc;
                }
                else {
                    *processed = buf_pos;
                    return copy_rc;
                }
            }
            zc->tx_msg_bytes_left -= copy_rc;
        }
        /*
         * repeat sending while there is some data in input or internal
         * compression buffer
         */
    } while (buf_pos < size);

    return buf_pos;
}

ssize_t zpq_c_read(ZpqController * zc, void *buf, size_t size) {
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

        if (!zpq_c_buffered_rx(zc)) {
            rc = zc->rx_func(zc->arg, (char*)zc->readahead_buf + zc->readahead_size, ZPQ_BUFFER_SIZE - zc->readahead_size);
            if (rc > 0) /* read fetches some data */
            {
                zc->readahead_size += rc;
            }
            else /* read failed */
            {
                return rc;
            }
        }

        /* try to prefetch the next message types and increase rx_msg_bytes_left, if possible */
        while (zc->rx_msg_bytes_left > 0 && (zc->readahead_size - zc->readahead_pos >= zc->rx_msg_bytes_left + 5)) {
            char msg_type;
            msg_type = *((char*)zc->readahead_buf + zc->readahead_pos + zc->rx_msg_bytes_left);
            if (zc->is_decompressing || zc_should_decompress(msg_type)) {
                break; // cannot proceed further
            }
            uint32 msg_len;
            memcpy(&msg_len, (char*)zc->readahead_buf + zc->readahead_pos + zc->rx_msg_bytes_left + 1, 4);
            zc->rx_msg_bytes_left += pg_ntoh32(msg_len) + 1;
        }


        if (zc->rx_msg_bytes_left > 0 || zpq_buffered_rx(zc->zs)) {
            if (zc->is_decompressing || zpq_buffered_rx(zc->zs)) {
                Assert(zc->readahead_pos <= zc->readahead_size);
                rx_processed = 0;
                buf_processed = 0;

                size_t read_count = zc->rx_msg_bytes_left;
                if (zc->readahead_size - zc->readahead_pos < read_count) {
                    read_count = zc->readahead_size - zc->readahead_pos;
                }
                rc = zpq_read(zc->zs, (char*)zc->readahead_buf + zc->readahead_pos, read_count, &rx_processed,
                              buf, size, &buf_processed);
                printf("decompressed_debug decompressed compr %zu raw %zu\n", rx_processed, buf_processed);
                fflush(stdout);
                zc->readahead_pos += rx_processed;
                buf_pos += buf_processed;
                zc->rx_msg_bytes_left -= rx_processed;
                if (rc == ZPQ_STREAM_END) {
                    continue;
                }
                if (rc != ZPQ_OK) {
                    return rc;
                }
            } else {
                Assert(zc->readahead_pos < zc->readahead_size);
                size_t copy_len = zc->rx_msg_bytes_left;
                if (zc->readahead_size - zc->readahead_pos < copy_len) {
                    copy_len = zc->readahead_size - zc->readahead_pos;
                }
                if (size < copy_len) {
                    copy_len = size;
                }
                memcpy(buf, (char*)zc->readahead_buf + zc->readahead_pos, copy_len);
                zc->readahead_pos += copy_len;
                buf_pos += copy_len;
                zc->rx_msg_bytes_left -= copy_len;
            }
        } else if (zc->readahead_size - zc->readahead_pos >= 5) {  /* determine next message type */
            /* read msg type and length if possible */
            char msg_type = zc->readahead_buf[zc->readahead_pos];
            zc->is_decompressing = zc_should_decompress(msg_type);

            uint32 msg_len;
            memcpy(&msg_len, zc->readahead_buf + zc-> readahead_pos + 1, 4);
            zc->rx_msg_bytes_left = pg_ntoh32(msg_len) + 1;

            if (zc->is_decompressing) {
                printf("decompressed_debug recv compressed msg, len %zu\n", zc->rx_msg_bytes_left);
                fflush(stdout);
                /* compressed message header is no longer needed, just skip it */
                zc->readahead_pos += 5;
                zc->rx_msg_bytes_left -= 5;
            }
        }
    }
    return buf_pos;
}

size_t zpq_c_buffered_rx(ZpqController * zc) {
    return zc ? zc->readahead_size - zc->readahead_pos >= 5 || (zc->readahead_size - zc->readahead_pos > 0 && zc->rx_msg_bytes_left > 0) || zpq_buffered_rx(zc->zs) : 0;
}

size_t zpq_c_buffered_tx(ZpqController * zc) {
    return zc ? zc->tx_msg_h_size == 5 || (zc->tx_msg_h_size > 0 && zc->tx_msg_bytes_left > 0) || zc->compressed_size - zc->compressed_pos > 0 || zpq_buffered_tx(zc->zs) : 0;
}

void
zpq_c_free(ZpqController * zc)
{
    if (zc)
    {
        if (zc->zs) {
            zpq_free(zc->zs);
        }
        free(zc);
    }
}

char const *
zpq_c_compress_error(ZpqController * zc)
{
    return zpq_compress_error(zc->zs);
}

char const *
zpq_c_decompress_error(ZpqController * zc)
{
    return zpq_decompress_error(zc->zs);
}

char const *
zpq_c_compress_algorithm_name(ZpqController * zc)
{
    return zpq_compress_algorithm_name(zc->zs);
}