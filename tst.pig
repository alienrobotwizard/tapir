input_data = load 'data.tsv' as (txt:chararray);

filtered = filter input_data by SIZE(TOKENIZE(txt)) > 3l;

store filtered into 'moved-notes';
