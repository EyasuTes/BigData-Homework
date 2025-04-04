#!/bin/bash

INPUT_DIR="Wikipedia-En-41784-Articles"
OUTPUT_DIR="outputs"
RESULT_FILE="benchmark_results.txt"

SUBFOLDERS=(AA AB AC AD AE AF AG AH AI AJ AK)

rm -f $RESULT_FILE
mkdir -p $OUTPUT_DIR

INPUT_PATH=""
echo "Benchmark Spark Jobs (WordFrequency, WordPairs, Top100Words, Top100NumWord)" >> $RESULT_FILE
echo "==========================================================" >> $RESULT_FILE

for folder in "${SUBFOLDERS[@]}"
do
  INPUT_PATH+="$INPUT_DIR/$folder,"
  CLEANED_INPUT_PATH=${INPUT_PATH%,}

  echo "\n[INPUT: $CLEANED_INPUT_PATH]" | tee -a $RESULT_FILE
  START=$(date +%s)

  # Launching for different questions
  spark-submit --class SparkWordCountb sparkwordcountb.jar "$CLEANED_INPUT_PATH" $OUTPUT_DIR >/dev/null 2>&1
  spark-submit --class SparkWordPairs sparkwordpairs.jar "$CLEANED_INPUT_PATH" $OUTPUT_DIR >/dev/null 2>&1
  spark-submit --class SparkTop100Words sparktop100words.jar "$CLEANED_INPUT_PATH" $OUTPUT_DIR >/dev/null 2>&1
  spark-submit --class SparkTop100NumWord sparktop100numword.jar "$CLEANED_INPUT_PATH" $OUTPUT_DIR >/dev/null 2>&1

  END=$(date +%s)
  DURATION=$((END - START))

  echo "Duration: $DURATION seconds" | tee -a $RESULT_FILE

done

echo "\nBenchmark completed. Results saved to $RESULT_FILE"

