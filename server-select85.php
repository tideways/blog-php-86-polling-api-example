<?php

require __DIR__ . '/common.php';

/**
 * Holds the connections together with the read or write “interest sets”
 * that stream_select() expects. stream_select() removes connections
 * without activity from the interest sets passed to it, preserving the
 * array keys of the remaining entries. The array keys are used to look
 * up the metadata from the ConnectionSet.
 */
final class ConnectionSet
{
    /**
     * Key of the listening socket in the read set.
     */
    public const int SERVER_ID = -1;

    /**
     * @var array<int, EchoConnection>
     */
    private array $conns = [];

    /**
     * @var array<int, resource>
     */
    private array $readSet;

    /**
     * @var array<int, resource>
     */
    private array $writeSet = [];

    private int $nextId = 0;

    public function __construct(
        Listener $listener,
        private readonly Stats $stats,
    ) {
        $this->readSet = [self::SERVER_ID => $listener->stream];
    }

    /**
     * @return array<int, resource>
     */
    public function readSet(): array
    {
        return $this->readSet;
    }

    /**
     * @return array<int, resource>
     */
    public function writeSet(): array
    {
        return $this->writeSet;
    }

    public function add(EchoConnection $conn): void
    {
        $id = $this->nextId;
        $this->nextId++;

        $this->conns[$id] = $conn;
        $this->readSet[$id] = $conn->stream;
        $this->stats->conns++;
    }

    /**
     * For connections that are known to be registered, such as everything the
     * read set hands back.
     */
    public function get(int $id): EchoConnection
    {
        return $this->conns[$id];
    }

    /**
     * For connections that may have been closed in the meantime.
     */
    public function getMaybe(int $id): ?EchoConnection
    {
        return $this->conns[$id] ?? null;
    }

    public function remove(int $id): void
    {
        $conn = $this->conns[$id];

        unset($this->conns[$id], $this->readSet[$id], $this->writeSet[$id]);
        $conn->close();
        $this->stats->conns--;
    }

    /**
     * Updates whether or not the connection is interested in being
     * included in the ->writeSet() due to echo response data still
     * being pending in internal buffers.
     */
    public function syncInterest(int $id): void
    {
        $conn = $this->conns[$id];

        if ($conn->hasPendingOutput()) {
            $this->writeSet[$id] = $conn->stream;
        } else {
            unset($this->writeSet[$id]);
        }
    }
}

$timeoutSeconds = 0;
$timeoutMicroseconds = 200_000;

$stats = new Stats('select85');
$listener = new Listener((int) ($argv[1] ?? 9501), $stats);
$connections = new ConnectionSet($listener, $stats);

for (;;) {
    $read = $connections->readSet();
    $write = $connections->writeSet();
    $except = null;

    $beforeWait = hrtime(true);
    $count = stream_select($read, $write, $except, $timeoutSeconds, $timeoutMicroseconds);
    $afterWait = hrtime(true);

    $stats->calls++;
    $stats->waitNs += $afterWait - $beforeWait;

    if ($count === false) {
        fwrite(STDERR, "stream_select() failed with {$stats->conns} connections open\n");
        $stats->report();

        exit(1);
    }

    foreach ($read as $id => $stream) {
        if ($id === ConnectionSet::SERVER_ID) {
            // The listening socket. Drain the accept queue.
            for (;;) {
                $accepted = $listener->accept();

                if ($accepted === null) {
                    break;
                }

                $connections->add($accepted);
            }
        } else {
            $conn = $connections->get($id);

            if ($conn->readAndEcho()) {
                $connections->syncInterest($id);
            } else {
                $connections->remove($id);
            }
        }
    }

    foreach ($write as $id => $stream) {
        $conn = $connections->getMaybe($id);

        if ($conn === null) {
            // Already closed while handling its read event.
            continue;
        }

        if ($conn->flush()) {
            $connections->syncInterest($id);
        } else {
            $connections->remove($id);
        }
    }

    $stats->workNs += hrtime(true) - $afterWait;
    $stats->maybeReport();
}
