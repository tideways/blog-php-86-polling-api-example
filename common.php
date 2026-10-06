<?php

/**
 * Implements the logic that is independent of the “readiness notification” logic
 * of the event loop and used by both implementations unchanged.
 */

/**
 * An echo server client connection.
 */
final class EchoConnection
{
    private const int CHUNK = 65536;

    /**
     * Buffered output that the socket would not accept yet.
     */
    private string $out = '';

    /**
     * @param resource $stream
     */
    public function __construct(
        public $stream,
        private readonly Stats $stats,
    ) {
        stream_set_blocking($stream, false);
        stream_set_read_buffer($stream, 0);
        stream_set_write_buffer($stream, 0);
    }

    public function hasPendingOutput(): bool
    {
        return $this->out !== '';
    }

    /**
     * Reads one chunk, stores it internally and calls flush().
     *
     * @return bool Whether the connection is still usable.
     */
    public function readAndEcho(): bool
    {
        $buf = @fread($this->stream, self::CHUNK);

        if ($buf === false) {
            return false;
        }

        if ($buf === '') {
            // Nothing to read. Either the peer has closed the connection, or
            // the readiness notification was stale.
            if (feof($this->stream)) {
                return false;
            }

            return true;
        }

        $this->stats->bytes += strlen($buf);
        $this->out .= $buf;

        return $this->flush();
    }

    /**
     * Write the buffered output to the socket. If the operating
     * system’s buffers are full, the remaining unflushed output
     * remains buffered.
     *
     * @return bool Whether the connection is still usable.
     */
    public function flush(): bool
    {
        while ($this->out !== '') {
            $written = @fwrite($this->stream, $this->out);

            if ($written === false) {
                return false;
            }

            if ($written === 0) {
                // The OS buffer is full.
                break;
            }

            $this->out = substr($this->out, $written);
        }

        return true;
    }

    public function close(): void
    {
        fclose($this->stream);
    }
}

final class Listener
{
    /**
     * @var resource
     */
    public $stream;

    public function __construct(
        int $port,
        private readonly Stats $stats,
    ) {
        $context = stream_context_create([
            'socket' => [
                'backlog' => 4096,
            ],
        ]);

        $server = stream_socket_server(
            "tcp://0.0.0.0:{$port}",
            $errno,
            $errstr,
            STREAM_SERVER_BIND | STREAM_SERVER_LISTEN,
            $context,
        );

        if ($server === false) {
            throw new RuntimeException("bind :{$port}: {$errstr} ({$errno})");
        }

        stream_set_blocking($server, false);
        fwrite(STDERR, "listening on :{$port}\n");

        $this->stream = $server;
    }

    /**
     * @return null|EchoConnection Null once the accept queue is empty.
     */
    public function accept(): ?EchoConnection
    {
        $client = @stream_socket_accept($this->stream, 0);

        if ($client === false) {
            return null;
        }

        return new EchoConnection($client, $this->stats);
    }
}

final class Stats
{
    public int $conns = 0;

    public int $calls = 0;

    public int $bytes = 0;

    public int $waitNs = 0;

    public int $workNs = 0;

    private int $lastReport;

    public function __construct(
        private readonly string $tag,
        private readonly int $reportingIntervalNs = 2_000_000_000,
    ) {
        $this->lastReport = hrtime(true);
    }

    public function maybeReport(): void
    {
        if (hrtime(true) - $this->lastReport < $this->reportingIntervalNs) {
            return;
        }

        $this->report();
    }

    public function report(): void
    {
        $now = hrtime(true);
        $seconds = max($now - $this->lastReport, 1) / 1e9;
        $calls = max($this->calls, 1);

        fprintf(
            STDERR,
            "%-9s conns=%5d %7.1f MiB/s %7.0f calls/s wait=%6.1f us/call work=%6.1f us/call\n",
            $this->tag,
            $this->conns,
            $this->bytes / 1048576 / $seconds,
            $this->calls / $seconds,
            $this->waitNs / $calls / 1000,
            $this->workNs / $calls / 1000,
        );

        $this->calls = 0;
        $this->bytes = 0;
        $this->waitNs = 0;
        $this->workNs = 0;
        $this->lastReport = $now;
    }
}
