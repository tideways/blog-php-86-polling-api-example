<?php

use Io\Poll\Event;
use Io\Poll\Watcher;

require __DIR__ . '/common.php';

/**
 * Updates whether or not the EchoConnection is interested in
 * the Event::Write event due to echo response data still
 * being pending in internal buffers.
 */
function syncInterest(Watcher $watcher, EchoConnection $conn): void
{
    $wantWrite = $conn->hasPendingOutput();
    $isRegistered = in_array(Event::Write, $watcher->getWatchedEvents(), true);

    // Check the currently registered events instead of unconditionally
    // calling `->modifyEvents()` to avoid running a syscall (`epoll_ctl()`)
    // for each connection.
    if ($wantWrite === $isRegistered) {
        return;
    }

    if ($wantWrite) {
        $watcher->modifyEvents([Event::Read, Event::Write]);
    } else {
        $watcher->modifyEvents([Event::Read]);
    }
}

$poll = new \Io\Poll\Context();
$timeout = \Time\Duration::fromMilliseconds(200);

$stats = new Stats(sprintf("poll86 (%s)", $poll->getBackend()->name));
$listener = new Listener((int) ($argv[1] ?? 9601), $stats);

// Store the $listener as the userland metadata to be able to directly
// access its methods when activity is reported without going through
// a manual lookup.
$poll->add(new StreamPollHandle($listener->stream), [Event::Read], $listener);

for (;;) {
    $beforeWait = hrtime(true);
    $ready = $poll->wait($timeout);
    $afterWait = hrtime(true);

    $stats->calls++;
    $stats->waitNs += $afterWait - $beforeWait;

    foreach ($ready as $watcher) {
        $subject = $watcher->getData();

        if ($subject instanceof Listener) {
            // The listening socket. Drain the accept queue.
            for (;;) {
                $accepted = $subject->accept();

                if ($accepted === null) {
                    break;
                }

                $poll->add(new StreamPollHandle($accepted->stream), [Event::Read], $accepted);
                $stats->conns++;
            }
        } elseif ($subject instanceof EchoConnection) {
            $alive = true;

            if ($watcher->hasTriggered(Event::Read)) {
                $alive = $subject->readAndEcho();
            }

            if ($alive && $watcher->hasTriggered(Event::Write)) {
                $alive = $subject->flush();
            }

            if ($watcher->hasTriggered(Event::Error) || $watcher->hasTriggered(Event::HangUp)) {
                $alive = false;
            }

            if ($alive) {
                syncInterest($watcher, $subject);
            } else {
                $watcher->remove();
                $subject->close();
                $stats->conns--;
            }
        }
    }

    $stats->workNs += hrtime(true) - $afterWait;
    $stats->maybeReport();
}
