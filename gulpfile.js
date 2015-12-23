var gulp   = require('gulp'),
    rename = require('gulp-rename'),
    uglify = require('gulp-uglify');

// Uglify for production
gulp.task('default', function(){
    return gulp.src('mapd-crossfilter.js')
      .pipe(uglify())
      .pipe(rename('mapd-crossfilter.min.js'))
      .pipe(gulp.dest('.'));
});

