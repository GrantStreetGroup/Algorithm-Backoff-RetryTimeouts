#!perl

use strict;
use warnings;
use Test::More 0.98;

use Algorithm::Backoff::RetryTimeouts;

my $rt;
my $time  = 0;
my $sqrt2 = sqrt(2);

subtest "Base defaults" => sub {
    $rt = Algorithm::Backoff::RetryTimeouts->new(
        # for the unit tests
        _start_timestamp => 0,
        jitter_factor    => 0,
    );

    $time = 0;
    is($rt->timeout, 25, 'Initial timeout: 25');

    # 1: one second attempt
    test_attempt(
        attempt_time   => 1,
        expected_delay => $sqrt2,  # sqrt(2)^1
    );

    # 2: instant failure
    test_attempt(
        attempt_time   => 0,
        expected_delay => 2,  # sqrt(2)^2
    );

    # 3: full timeout
    test_attempt(
        attempt_time   => $rt->timeout,
        expected_delay => 0,
    );

    # 4: one second attempt
    test_attempt(
        attempt_time   => 1,
        expected_delay => 3,  # sqrt(2)^4 - 1
    );

    # 5: full timeout (with min_adjust_timeout trigger)
    test_attempt(
        expected_delay   => 0,
        expected_timeout => 5,
    );

    # 6: full timeout (with remaining time max delay check)
    test_attempt(
        expected_delay   => 2.198,  # 50% of the remaining time
        expected_timeout => 5,
    );

    # 7: final attempt
    test_attempt(
        expected_delay   => -1,
        expected_timeout => 5
    );
};

subtest "attr: adjust_timeout_factor" => sub {
    $rt = Algorithm::Backoff::RetryTimeouts->new(
        adjust_timeout_factor => 0.25,

        # for the unit tests
        _start_timestamp => 0,
        jitter_factor    => 0,
    );

    $time = 0;
    is($rt->timeout, 12.5, 'Initial timeout: 12.5');

    # 1: one second attempt
    test_attempt(
        attempt_time   => 1,
        expected_delay => $sqrt2,  # sqrt(2)^1
    );

    # 2: instant failure
    test_attempt(
        attempt_time   => 0,
        expected_delay => 2,  # sqrt(2)^2
    );

    # 3: full timeout
    test_attempt(
        expected_delay => 0,
    );

    # 4: one second attempt
    test_attempt(
        attempt_time   => 1,
        expected_delay => 3,  # sqrt(2)^4 - 1
    );

    # 5: full timeout
    test_attempt(
        expected_delay => 0,
    );

    # 6: full timeout (with min_adjust_timeout trigger)
    test_attempt(
        expected_delay   => 2.339,  # sqrt(2)^6 = 8 - 5.661 (prev timeout)
        expected_timeout => 5,
    );

    # 7: full timeout
    test_attempt(
        expected_delay   => 6.314,  # sqrt(2)^7 - 5
        expected_timeout => 5,
    );

    # 8: final attempt
    test_attempt(
        expected_delay   => -1,
        expected_timeout => 5,
    );
};

subtest "attr: min_adjust_timeout" => sub {
    $rt = Algorithm::Backoff::RetryTimeouts->new(
        adjust_timeout_factor => 0.75,  # just to make this faster
        min_adjust_timeout    => 0,

        # for the unit tests
        _start_timestamp => 0,
        jitter_factor    => 0,
    );

    $time = 0;
    is($rt->timeout, 37.5, 'Initial timeout: 37.5');

    # 1: full timeout
    test_attempt(
        expected_delay => $sqrt2,  # sqrt(2)^1
    );

    # 2: full timeout
    test_attempt(
        expected_delay => 0,
    );

    # NOTE: The rest of these are so close to the edge of max_actual_duration that they
    # consistently hit the remaining time max delay check.

    # 3-7: full timeouts
    test_attempt(
        expected_delay => 0.173,
    );
    test_attempt(
        expected_delay => 0.032,
    );
    test_attempt(
        expected_delay => 0.006,
    );
    test_attempt(
        expected_delay => 0.001,
    );
    test_attempt(
        expected_delay => 0,
    );

    # 8: final attempt
    test_attempt(
        expected_delay => -1,
    );
};

done_testing;

sub test_attempt {
    my (%args) = @_;

    # Progress the timestamp
    $time += $rt->delay;
    $time += $args{attempt_time} // $rt->timeout;

    # Fail or succeed
    my $method = $args{method} // 'failure';

    my ($delay, $timeout) = $rt->$method($time);
    my $attempts = $rt->{_attempts};

    # Figure out the expected values
    my $expected_delay   = round($args{expected_delay});
    my $expected_timeout = round(
        $args{expected_timeout} // (
            ($rt->{max_actual_duration} - $time - $rt->delay) * $rt->{adjust_timeout_factor}
        )
    );

    # Run the unit tests
    diag "Time: ".round($time).", Attempt \#$attempts: $method";
    is(
        round($delay),
        $expected_delay,
        "Expected delay: $expected_delay",
    );
    is(
        round($timeout),
        $expected_timeout,
        "Expected timeout: $expected_timeout",
    );
    is($delay,   $rt->delay,   'Delay   method matches return') unless $delay == -1;
    is($timeout, $rt->timeout, 'Timeout method matches return');
}

sub round { sprintf("%.3f", shift) + 0; }
