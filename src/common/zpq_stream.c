#include "postgres_fe.h"
#include "common/zpq_stream.h"
#include "c.h"
#include "pg_config.h"

//#define HAVE_LIBZSTD 1
//#define HAVE_LIBZ 1

/*
 * Functions implementing streaming compression algorithm
 */
typedef struct
{
	/*
	 * Returns name of compression algorithm.
	 */
	char const* (*name)(void);

	/*
	 * Create compression stream with using rx/tx function for fetching/sending compressed data.
	 * level: compression level
	 */
	void* (*create)(int level);

	/*
	 * Decompress up to "size" raw (decompressed) bytes.
	 * Return code > 0: number of decompressed bytes.
	 * Return code < 0: error code
	 * Error code is either ZPQ_DECOMPRESS_ERROR, ZPQ_STREAM_END, ZPQ_NEED_MORE_DATA
	 * or error code returned by the decompressor function.
	 */
    ssize_t (*decompress)(void *zs, void *rx_buf, size_t rx_size, size_t *rx_processed, void *buf, size_t size, size_t *buf_processed);

	/*
	 * Write up to "size" raw (decompressed) bytes.
	 * Returns number of written raw bytes or error code returned by tx function.
	 * In the last case amount of written raw bytes is stored in *processed.
	 */
    ssize_t (*compress)(void *zs, void *tx_buf, size_t tx_size, size_t *tx_processed, void const *buf, size_t size, size_t *buf_processed);

	/*
	 * Free stream created by create function.
	 */
	void    (*free)(void *zs);

	/*
	 * Get error message.
	 */
	char const* (*error)(void *zs);
} ZpqAlgorithm;


#define ZPQ_BUFFER_SIZE (8*1024)


#if HAVE_LIBZSTD

#include <stdlib.h>
#include <zstd.h>

/*
 * Maximum allowed back-reference distance, expressed as power of 2.
 * This setting controls max compressor/decompressor window size.
 * More details https://github.com/facebook/zstd/blob/v1.4.7/lib/zstd.h#L536
 */
#define ZSTD_WINDOWLOG_LIMIT 23 /* set max window size to 8MB */

typedef struct ZstdStream
{
	ZSTD_CStream*  tx_stream;
	ZSTD_DStream*  rx_stream;
	char const*    error;    /* error message */
} ZstdStream;

static void*
zstd_create(int level)
{
	ZstdStream* zs = (ZstdStream*)malloc(sizeof(ZstdStream));

	zs->tx_stream = ZSTD_createCStream();
	ZSTD_initCStream(zs->tx_stream, level);
	ZSTD_CCtx_setParameter(zs->tx_stream, ZSTD_c_windowLog, ZSTD_WINDOWLOG_LIMIT);
	zs->rx_stream = ZSTD_createDStream();
	ZSTD_initDStream(zs->rx_stream);
	ZSTD_DCtx_setParameter(zs->rx_stream, ZSTD_d_windowLogMax, ZSTD_WINDOWLOG_LIMIT);

	zs->error = NULL;
	return zs;
}

static ssize_t
zstd_read(void *zstream, void *rx_buf, size_t rx_size, size_t *rx_processed, void *buf, size_t size, size_t *buf_processed)
{
    ZstdStream* zs = (ZstdStream*)zstream;
    ZSTD_inBuffer in;
    in.src = rx_buf;
    in.pos = 0;
    in.size = rx_size;
	ZSTD_outBuffer out;
	out.dst = buf;
	out.pos = 0;
	out.size = size;

    size_t rc = ZSTD_decompressStream(zs->rx_stream, &out, &in);
    *rx_processed = in.pos;
    *buf_processed = out.pos;
    if (ZSTD_isError(rc))
    {
        zs->error = ZSTD_getErrorName(rc);
        return ZPQ_DECOMPRESS_ERROR;
    }

    if (out.pos == out.size) {
        /*  if `output.pos == output.size`, there might be some data left within internal buffers */
        return ZPQ_DATA_PENDING;
    }
    return ZPQ_OK;
}

static ssize_t
zstd_write(void *zstream, void *tx_buf, size_t tx_size, size_t *tx_processed, void const *buf, size_t size, size_t *buf_processed)
{
    ZstdStream* zs = (ZstdStream*)zstream;
    ZSTD_outBuffer out;
    out.dst = tx_buf;
    out.pos = 0;
    out.size = tx_size;
    ZSTD_inBuffer in;
    in.src = buf;
    in.pos = 0;
    in.size = size;

    if (in.pos < size) /* Has something to compress in input buffer */
    {
        size_t rc = ZSTD_compressStream(zs->tx_stream, &out, &in);
        *tx_processed = out.pos;
        *buf_processed = in.pos;
        if (ZSTD_isError(rc))
        {
            zs->error = ZSTD_getErrorName(rc);
            return ZPQ_COMPRESS_ERROR;
        }
    }

    if (in.pos == size) /* All data is compressed: flush internal zstd buffer */
    {
        size_t tx_not_flushed = ZSTD_flushStream(zs->tx_stream, &out);
        *tx_processed = out.pos;
        if (tx_not_flushed > 0) {
            return ZPQ_DATA_PENDING;
        }
    }

    return ZPQ_OK;
}

static void
zstd_free(void *zstream)
{
    ZstdStream* zs = (ZstdStream*)zstream;
    if (zs != NULL)
	{
		ZSTD_freeCStream(zs->tx_stream);
		ZSTD_freeDStream(zs->rx_stream);
		free(zs);
	}
}

static char const*
zstd_error(void *zstream)
{
    ZstdStream* zs = (ZstdStream*)zstream;
    return zs->error;
}

static char const*
zstd_name(void)
{
	return "zstd";
}

#endif

#if HAVE_LIBZ

#include <stdlib.h>
#include <zlib.h>

#define ZLIB_BUFFER_SIZE       8192 /* We have to flush stream after each protocol command
									 * and command is mostly limited by record length,
									 * which in turn usually less than page size (except TOAST)
									 */

typedef struct ZlibStream
{
    z_stream tx;
	z_stream rx;
} ZlibStream;

static void*
zlib_create(int level)
{
	int rc;
	ZlibStream* zs = (ZlibStream*)malloc(sizeof(ZlibStream));
	memset(&zs->tx, 0, sizeof(zs->tx));
	zs->tx.avail_out = ZLIB_BUFFER_SIZE;
	rc = deflateInit(&zs->tx, level);
	if (rc != Z_OK)
	{
		free(zs);
		return NULL;
	}
	Assert(zs->tx.avail_out == ZLIB_BUFFER_SIZE);

	memset(&zs->rx, 0, sizeof(zs->tx));
	zs->rx.avail_in = ZLIB_BUFFER_SIZE;
	rc = inflateInit(&zs->rx);
	if (rc != Z_OK)
	{
		free(zs);
		return NULL;
	}
	Assert(zs->rx.avail_in == ZLIB_BUFFER_SIZE);

	return zs;
}

static ssize_t
zlib_read(void *zstream, void *rx_buf, size_t rx_size, size_t *rx_processed, void *buf, size_t size, size_t *buf_processed)
{
    ZlibStream* zs = (ZlibStream*)zstream;
	int rc;
	zs->rx.next_in = rx_buf;
    zs->rx.avail_in = rx_size;
    zs->rx.next_out = (Bytef *)buf;
    zs->rx.avail_out = size;

    rc = inflate(&zs->rx, Z_SYNC_FLUSH);
    *rx_processed = rx_size - zs->rx.avail_in;
    *buf_processed = size - zs->rx.avail_out;

    if (rc == Z_STREAM_END) {
        return ZPQ_STREAM_END;
    }
    if (rc != Z_OK && rc != Z_BUF_ERROR)
    {
        return ZPQ_DECOMPRESS_ERROR;
    }

    return ZPQ_OK;
}

static ssize_t
zlib_write(void *zstream, void *tx_buf, size_t tx_size, size_t *tx_processed, void const *buf, size_t size, size_t *buf_processed)
{
	ZlibStream* zs = (ZlibStream*)zstream;
    int rc;
    zs->tx.next_out = (Bytef *)tx_buf;
    zs->tx.avail_out = tx_size;
	zs->tx.next_in = (Bytef *)buf;
	zs->tx.avail_in = size;

    rc = deflate(&zs->tx, Z_SYNC_FLUSH);
    Assert(rc == Z_OK);
    *tx_processed = tx_size - zs->tx.avail_out;
    *buf_processed = size - zs->tx.avail_in;

    unsigned deflate_pending = 0;
    deflatePending(&zs->tx, &deflate_pending, Z_NULL); /* check if any data left in deflate buffer */
    if (deflate_pending > 0) {
        return ZPQ_DATA_PENDING;
    }
    return ZPQ_OK;
}

static void
zlib_free(void *zstream)
{
	ZlibStream* zs = (ZlibStream*)zstream;
	if (zs != NULL)
	{
		inflateEnd(&zs->rx);
		deflateEnd(&zs->tx);
		free(zs);
	}
}

static char const*
zlib_error(void *zstream)
{
	ZlibStream* zs = (ZlibStream*)zstream;
	return zs->rx.msg;
}


static char const*
zlib_name(void)
{
	return "zlib";
}

#endif

static char const*
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
	{zstd_name, zstd_create, zstd_read, zstd_write, zstd_free, zstd_error},
#endif
#if HAVE_LIBZ
	{zlib_name, zlib_create, zlib_read, zlib_write, zlib_free, zlib_error},
#endif
	{no_compression_name}
};

struct ZpqStream
{
    ZpqAlgorithm const* algorithm;
    void*               stream;

    char           tx_buf[ZPQ_BUFFER_SIZE];
    size_t         tx_pos;
    size_t         tx_size;

    char           rx_buf[ZPQ_BUFFER_SIZE];
    size_t         rx_pos;
    size_t         rx_size;

    zpq_tx_func    tx_func;
    zpq_rx_func    rx_func;
    void*          arg;

    size_t         tx_total;
    size_t         tx_total_raw;
    size_t         rx_total;
    size_t         rx_total_raw;

    bool           rx_not_flushed;
    bool           tx_not_flushed;
};

/*
 * Index of used compression algorithm in zpq_algorithms array.
 */
ZpqStream*
zpq_create(int algorithm_impl, int level, zpq_tx_func tx_func, zpq_rx_func rx_func, void *arg, char* rx_data, size_t rx_data_size)
{
//sleep(30);
    ZpqStream* zs = (ZpqStream*)malloc(sizeof(ZpqStream));
    zs->tx_pos = 0;
    zs->tx_size = 0;
    zs->rx_pos = 0;
    zs->rx_size = 0;
    zs->tx_func = tx_func;
    zs->rx_func = rx_func;
    zs->arg = arg;
    zs->tx_total = 0;
    zs->tx_total_raw = 0;
    zs->rx_total = 0;
    zs->rx_total_raw = 0;
    zs->tx_not_flushed = false;
    zs->rx_not_flushed = false;

    zs->rx_size = rx_data_size;
    Assert(rx_data_size < ZPQ_BUFFER_SIZE);
    memcpy(zs->rx_buf, rx_data, rx_data_size);

    zs->algorithm = &zpq_algorithms[algorithm_impl];
    zs->stream = zpq_algorithms[algorithm_impl].create(level);

	return zs;
}

ssize_t
zpq_read(ZpqStream *zs, void *buf, size_t size)
{
    size_t buf_pos = 0;

    while (buf_pos == 0) { /* Read until some data fetched */
        if (zs->rx_pos == zs->rx_size) {
            zs->rx_pos = zs->rx_size = 0; /* Reset rx buffer */
        }

        if (zs->rx_pos == zs->rx_size && !zs->rx_not_flushed) {
            ssize_t rc = zs->rx_func(zs->arg, (char*)zs->rx_buf + zs->rx_size, ZPQ_BUFFER_SIZE - zs->rx_size);
            if (rc > 0) /* read fetches some data */
            {
                zs->rx_size += rc;
                zs->rx_total += rc;
            }
            else /* read failed */
            {
                return rc;
            }
        }

        Assert(zs->rx_pos <= zs->rx_size);
        size_t rx_processed = 0;
        size_t buf_processed = 0;
        ssize_t rc = zs->algorithm->decompress(zs->stream,
                                           (char*)zs->rx_buf + zs->rx_pos, zs->rx_size - zs->rx_pos, &rx_processed,
                                           buf, size, &buf_processed);
        zs->rx_pos += rx_processed;
        zs->rx_total_raw += rx_processed;
        buf_pos += buf_processed;
        zs->rx_not_flushed = false;

        if (rc == ZPQ_DATA_PENDING) {
            zs->rx_not_flushed = true;
            continue;
        }
        if (rc != ZPQ_OK) {
            return ZPQ_DECOMPRESS_ERROR;
        }
    }
    return buf_pos;
}

ssize_t
zpq_write(ZpqStream *zs, void const *buf, size_t size, size_t* processed)
{
    size_t buf_pos = 0;
    do
    {
        if (zs->tx_pos == zs->tx_size) /* Have nothing to send */
        {
            zs->tx_pos = zs->tx_size = 0; /* Reset pointer to the beginning of buffer */

            size_t tx_processed = 0;
            size_t buf_processed = 0;
            ssize_t rc = zs->algorithm->compress(zs->stream,
                                             (char*)zs->tx_buf + zs->tx_size, ZPQ_BUFFER_SIZE - zs->tx_size, &tx_processed,
                                             (char*)buf + buf_pos, size - buf_pos, &buf_processed);

            zs->tx_size += tx_processed;
            buf_pos += buf_processed;
            zs->tx_total_raw += buf_processed;
            zs->tx_not_flushed = false;

            if (rc == ZPQ_DATA_PENDING) {
                zs->tx_not_flushed = true;
                continue;
            }
            if (rc != ZPQ_OK) {
                *processed = buf_pos;
                return ZPQ_COMPRESS_ERROR;
            }
        }
        while(zs->tx_pos < zs->tx_size) {
            ssize_t rc = zs->tx_func(zs->arg, (char*)zs->tx_buf + zs->tx_pos, zs->tx_size - zs->tx_pos);
            if (rc > 0)
            {
                zs->tx_pos += rc;
                zs->tx_total += rc;
            }
            else
            {
                *processed = buf_pos;
                return rc;
            }
        }
        /* repeat sending while there is some data in input or internal zstd buffer */
    } while (buf_pos < size || zs->tx_not_flushed);

    return buf_pos;
}

void
zpq_free(ZpqStream *zs)
{
	if (zs) {
	    if (zs->stream) {
            zs->algorithm->free(zs->stream);
	    }
	    free(zs);
	}
}

char const*
zpq_error(ZpqStream *zs)
{
	return zs->algorithm->error(zs->stream);
}


size_t
zpq_buffered_rx(ZpqStream *zs)
{
	return zs ? zs->rx_not_flushed || (zs->rx_size - zs->rx_pos) : 0;
}

size_t
zpq_buffered_tx(ZpqStream *zs)
{
	return zs ? zs->tx_not_flushed || (zs->tx_size - zs->tx_pos) : 0;
}

/*
 * Get list of the supported algorithms.
 */
char**
zpq_get_supported_algorithms(void)
{
	size_t n_algorithms = sizeof(zpq_algorithms)/sizeof(*zpq_algorithms);
	char** algorithm_names = malloc(n_algorithms*sizeof(char*));

	for (size_t i = 0; i < n_algorithms; i++)
	{
		algorithm_names[i] = (char*)zpq_algorithms[i].name();
	}

	return algorithm_names;
}

char const*
zpq_algorithm_name(ZpqStream *zs)
{
	return zs ? zs->algorithm->name() : NULL;
}
