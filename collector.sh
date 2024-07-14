export COLLECTOR_HOME=/usr/local/sw/collector

function collector
{
    command pushd "$COLLECTOR_HOME" >/dev/null
    python collector.py $@
    command popd >/dev/null
}
