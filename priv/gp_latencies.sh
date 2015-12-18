#!/bin/bash

function Usage {
  echo "Usage: gp_latencies.sh [-d TEST_DIR] [-o OPERATIONS] [-k STATS_KINDS]" >&2
  echo "                       [-t TERMINAL_TYPE] [-s PLOT_STYLE] [-p PRE_COMMAD]" >&2
  echo "                       [-P] [-E EXEC_COMMAND] [-h]" >&2
  echo "" >&2
  echo "    -d TEST_DIR:      comma separated test directories with *-latencies.csv" >&2
  echo "                      default: \"tests/current\"" >&2
  echo "    -o OPERATIONS:    operation prefixes of *-latencies.csv" >&2
  echo "                      in comma separated list" >&2
  echo "                      default: all operations under TEST_DIR" >&2
  echo "    -k STATS_KINDS:   statistics kinds in comma separated list" >&2
  echo "                      default: \"99th,mean\"" >&2
  echo "    -t TERMINAL_TYPE: terminal type of gnuplot (e.g. dumb, x11, aqua)" >&2
  echo "                      default: nothing" >&2
  echo "    -s PLOT_STYLE:    plot style for points and lines, etc" >&2
  echo "                      default: \"linespoints pointsize 2 linewidth 1\"" >&2
  echo "    -p PRE_COMMAND:   any command to be executed before plot" >&2
  echo "                      default: nothing" >&2
  echo "    -E EXEC_COMMAND:  command to be executed" >&2
  echo "                      default: \"gnuplot -persist\"" >&2
  echo "    -P:               print gnuplot script to stdout" >&2
  echo "    -h:               print this usage" >&2
  exit 1
}

TEST_DIR="tests/current"
OPERATIONS=
STATS_KINDS="99th,mean"
TERMINAL_COMMAND=
PLOT_STYLE="linespoints pointsize 2 linewidth 1"
PRE_COMMAD=
EXEC_COMMAND="gnuplot -persist"

while getopts ":d:o:k:t:s:p:PE:h" opt; do
    case $opt in
        d)
            TEST_DIR=${OPTARG} ;;
        o)
            OPERATIONS=${OPTARG} ;;
        k)
            STATS_KINDS=${OPTARG} ;;
        t)
            TERMINAL_COMMAND="set terminal ${OPTARG}" ;;
        s)
            PLOT_STYLE=${OPTARG} ;;
        p)
            PRE_COMMAD=${OPTARG} ;;
        P)
            EXEC_COMMAND="cat" ;;
        E)
            EXEC_COMMAND=${OPTARG} ;;
        h)
            Usage ;;
        \?)
            echo "Invalid option: -${OPTARG}" >&2
            Usage ;;
        :)
            echo "Option -${OPTARG} requires an argument." >&2
            Usage ;;
    esac
done

function plot_command(){
    echo "plot \\"
    for THIS_TEST_DIR in ${TEST_DIR//,/ }
    do
        if [ -z "${OPERATIONS}" ]; then
            for f in `ls ${THIS_TEST_DIR}/*_latencies.csv`
            do
                OPERATION=`basename $f _latencies.csv`
                plot_per_op ${OPERATION}
            done
        else
            for OPERATION in ${OPERATIONS//,/ }
            do
                plot_per_op ${OPERATION}
            done
        fi
    done
    echo "    1/0 notitle # dummy"
}

function plot_per_op(){
    OPERATION=$1
    LATENCY_FILE="${THIS_TEST_DIR}/${OPERATION}_latencies.csv"
    for KIND in ${STATS_KINDS//,/ }
    do
        plot_per_op_kind ${OPERATION} ${LATENCY_FILE} ${KIND}
    done
}

function plot_per_op_kind() {
    OPERATION=$1
    FILE=$2
    KIND=$3
    DISPLAY_SCALE=1000
    case "${KIND}" in
        "min")    COL_POS=4 ;;
        "mean")   COL_POS=5 ;;
        "medean") COL_POS=6 ;;
        "95th")   COL_POS=7 ;;
        "99th")   COL_POS=8 ;;
        "99.9th") COL_POS=9 ;;
        "max")    COL_POS=10 ;;
        "errors")
            # This column is count, not time duration.
            # Be Careful about / Don't get fooled by vertical axis label.
            # It may be useful to use options "-k errors" to dispaly
            # this column only and "-p" to set the label.
            COL_POS=11
            DISPLAY_SCALE=1 ;;
        *)        Usage ;;
    esac
    echo "    \"${FILE}\" using 1:(\$${COL_POS}/${DISPLAY_SCALE}) with \\"
    echo "        ${PLOT_STYLE} \\"
    # If plotting only 1 directory, do not add its name
    if [ "${TEST_DIR}" == "${THIS_TEST_DIR}" ]; then
        echo "        title \"${OPERATION}:${KIND}\" \\"
    else
        echo "        title \"${THIS_TEST_DIR##*/} - ${OPERATION}:${KIND}\" \\"
    fi
    echo "        ,\\"
}

PLOT_COMMAND=`plot_command`

${EXEC_COMMAND} << EOF

## Use terminal definition as you like (via -t option)
# set terminal dumb                # character terminal
# set terminal dumb 79 49 enhanced # character terminal portrait
# set terminal x11                 # X-Window
# set terminal x11 persist         # X-Window, remain alive after gnuplot exits
# set terminal aqua                # AquaTerm
${TERMINAL_COMMAND}

## title, key and axis
set title "Latency [msec]"
set autoscale
set yrange [0:]
set grid
set xlabel "Elapsed [sec]"
set ylabel "Latency [msec]"
set key inside bottom

## data file
set datafile separator ','

${PRE_COMMAD}

## plot
${PLOT_COMMAND}

EOF
