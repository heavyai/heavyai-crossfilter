./node_modules/gulp/bin/gulp.js | tee build_output.txt
grep -q -i 'error' build_output.txt
if [ "$?" == "0" ]; then
  exit 1
fi

rm build_output.txt

