hadoop jar cc.jar cc.FilterTextHtml \
 -libjars nutch-1.2.jar,tika-app-1.0.jar \
 arc_files.$1/*/*/*/*/* text_html.$1
