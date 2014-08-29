
if [ "$1" == "json" ]; then
    json="json=1"
    shift
elif  [ "$1" == "csv" ]; then
    json="csv=1"
    shift
else
    json="bash=1"
fi

if [ "$1" == "script" ]; then
    fail="-f"
    shift
else
	fail=""
fi

curl ${fail} -k -s --data-urlencode "sync=sync" --data-urlencode "username=${USER}" --data-urlencode ${json} --data-urlencode "client=bash" --data-urlencode "q=$1" ${SERVER_URL}