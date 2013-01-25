#!/bin/bash

function Usage {
  echo "Usage: gp_throughput.sh [-d TEST_DIR] [-k SUMMARY_KINDS]" >&2
  echo "                        [-t TERMINAL_TYPE] [-s PLOT_STYLE] [-p PRE_COMMAD]" >&2
  echo "                        [-P] [-h]" >&2
  echo "" >&2
  echo "    -d TEST_DIR:      test directory with summary.csv" >&2
  echo "                      default: \"tests/current\"" >&2
  echo "    -k SUMMARY_KINDS: summary kinds in comma separated list" >&2
  echo "                      default: \"total,failed\"" >&2
  echo "    -t TERMINAL_TYPE: terminal type of gnuplot (e.g. dumb, x11, aqua)" >&2
  echo "                      default: nothing" >&2
  echo "    -s PLOT_STYLE:    plot style for points and lines, etc" >&2
  echo "                      default: \"linespoints pointsize 2 linewidth 1\"" >&2
  echo "    -p PRE_COMMAND:   any command to be executed before plot" >&2
  echo "                      default: nothing" >&2
  echo "    -P:               print gnuplot script to stdout" >&2
  echo "    -h:               print this usage" >&2
  exit 1
}

TEST_DIR=tests/current
SUMMARY_KINDS="total,failed"
TERMINAL_COMMAND=
PLOT_STYLE="linespoints pointsize 2 linewidth 1"
PRE_COMMAD=
EXEC_COMMAND="gnuplot -persist"

while getopts ":d:k:t:s:p:Ph" opt; do
    case $opt in
        d)
            TEST_DIR=${OPTARG} ;;
        k)
            SUMMARY_KINDS=${OPTARG} ;;
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
    for KIND in ${SUMMARY_KINDS//,/ }
    do
        plot_per_kind ${KIND}
    done
    echo "    1/0 notitle # dummy"
}

function plot_per_kind() {
    FILE=${TEST_DIR}/summary.csv
    case "${KIND}" in
        "total")      COL_POS=3 ;;
        "successful") COL_POS=4 ;;
        "failed")     COL_POS=5 ;;
        *)            Usage ;;
    esac
    echo "    \"${FILE}\" using 1:(\$${COL_POS}/\$2) with \\"
    echo "        ${PLOT_STYLE} \\"
    echo "        title \"${KIND}\" \\"
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
set title "Throughput [ops/sec]"
set autoscale
set yrange [0:]
set grid
set xlabel "Elapsed [sec]"
set ylabel "ops/sec"
set key inside bottom

## data file
set datafile separator ','

${PRE_COMMAD}

## plot
${PLOT_COMMAND}

EOF
