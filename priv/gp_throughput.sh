#!/bin/bash

function Usage {
  echo "Usage: gp_throughput.sh [-d TEST_DIR] [-k SUMMARY_KINDS] [-u UNIT]" >&2
  echo "                        [-t TERMINAL_TYPE] [-s PLOT_STYLE] [-p PRE_COMMAD]" >&2
  echo "                        [-P] [-E EXEC_COMMAND] [-h]" >&2
  echo "" >&2
  echo "    -d TEST_DIR:      comma separated test directories with summary.csv" >&2
  echo "                      default: \"tests/current\"" >&2
  echo "    -k SUMMARY_KINDS: summary kinds in comma separated list" >&2
  echo "                      default: \"total,failed\"" >&2
  echo "    -u UNIT:          unit of measurement" >&2
  echo "                      default: \"ops/sec\"" >&2
  echo "    -t TERMINAL_TYPE: terminal type of gnuplot (e.g. dumb, x11, aqua)" >&2
  echo "                      default: nothing" >&2
  echo "    -s PLOT_STYLE:    plot style for points and lines, etc" >&2
  echo "                      default: \"linespoints pointsize 2 linewidth 1\"" >&2
  echo "    -p PRE_COMMAND:   any command to be executed before plot" >&2
  echo "                      default: nothing" >&2
  echo "    -P:               print gnuplot script to stdout" >&2
  echo "    -E EXEC_COMMAND:  command to be executed" >&2
  echo "                      default: \"gnuplot -persist\"" >&2
  echo "    -h:               print this usage" >&2
  exit 1
}

TEST_DIR=tests/current
SUMMARY_KINDS="total,failed"
TERMINAL_COMMAND=
PLOT_STYLE="linespoints pointsize 2 linewidth 1"
PRE_COMMAD=
EXEC_COMMAND="gnuplot -persist"
UNIT="ops/sec"

while getopts ":d:k:u:t:s:p:PE:h" opt; do
    case $opt in
        d)
            TEST_DIR=${OPTARG} ;;
        k)
            SUMMARY_KINDS=${OPTARG} ;;
        u)
            UNIT="${OPTARG}" ;;
        t)
            TERMINAL_COMMAND="set terminal ${OPTARG}" ;;
        s)
            PLOT_STYLE=${OPTARG} ;;
        p)
            PRE_COMMAD=${OPTARG} ;;
        P)
            EXEC_COMMAND="cat" ;;
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
        for KIND in ${SUMMARY_KINDS//,/ }
        do
            plot_per_kind ${KIND}
        done
    done
    echo "    1/0 notitle # dummy"
}

function plot_per_kind() {
    FILE=${THIS_TEST_DIR}/summary.csv
    case "${KIND}" in
        "total")      COL_POS=3 ;;
        "successful") COL_POS=4 ;;
        "failed")     COL_POS=5 ;;
        *)            Usage ;;
    esac
    echo "    \"${FILE}\" using 1:(\$${COL_POS}/\$2) with \\"
    echo "        ${PLOT_STYLE} \\"

    # If plotting only 1 directory, do not add its name
    if [ "${THIS_DIR}" == "${THIS_TEST_DIR}" ]; then
        echo "        title \"${KIND}\" \\"
    else
        echo "        title \"${THIS_TEST_DIR##*/} - ${KIND}\" \\"
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
set title "Throughput ${UNIT}"
set autoscale
set yrange [0:]
set grid
set xlabel "Elapsed [sec]"
set ylabel "${UNIT}"
set key inside bottom

## data file
set datafile separator ','

${PRE_COMMAD}

## plot
${PLOT_COMMAND}

EOF
