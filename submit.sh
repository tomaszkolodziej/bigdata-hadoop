usage() {
  echo -e "Usage: $0 [-i <path>] [-o <path>]\n"\
       "where\n"\
       "-i defines an input path\n"\
       "-o defines an output path\n"\
       "-e defines an executor: hadoop or yarn, yarn but default\n"\
       "\n"\
        1>&2
  exit 1
}


while getopts ":i:o:e:" opt; do
    case "$opt" in
        i)  INPUT_PATH=${OPTARG} ;;
        o)  OUTPUT_PUTH=${OPTARG} ;;
        e)  EXECUTOR=${OPTARG} ;;
        *)  usage ;;
    esac
done

if [[ -z "$INPUT_PATH" ]];
then
  INPUT_PATH="/flights/flights.csv"
fi

if [[ -z "$OUTPUT_PATH" ]];
then
  OUTPUT_PATH="/flights/output"
fi

if [[ -z "$EXECUTOR" ]];
then
  EXECUTOR="yarn"
fi

hadoop fs -rm -R $OUTPUT_PATH
hdfs dfs -ls ${INPUT_PATH}

THIS_FILE=$(readlink -f "$0")
THIS_PATH=$(dirname "$THIS_FILE")
BASE_PATH=$(readlink -f "$THIS_PATH/../")
APP_PATH="$THIS_PATH/source/flights-1.0-jar-with-dependencies.jar"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
echo "THIS_FILE = $THIS_FILE"
echo "THIS_PATH = $THIS_PATH"
echo "BASE_PATH = $BASE_PATH"
echo "APP_PATH = $APP_PATH"
echo "-------------------------------------"
echo "INPUT_PATH = $INPUT_PATH"
echo "OUTPUT_PATH = $OUTPUT_PATH"
echo "-------------------------------------"

mapReduceArguments=(
  "$APP_PATH"
  "com.globallogic.bdpc.flights.Flights"
  "$INPUT_PATH"
  "$OUTPUT_PATH"
  "$OUTPUT_PATH_2"
)

SUBMIT_CMD="${EXECUTOR} jar ${mapReduceArguments[@]}"
echo "$SUBMIT_CMD"
${SUBMIT_CMD}

echo "You should find results here: 'hadoop fs -ls $OUTPUT_PATH'"
echo "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<"
