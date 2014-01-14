input_data = load 'notes.txt' as (first_field:chararray);

filtered = filter input_data by SIZE(TOKENIZE(first_field)) > 3l;

store filtered into 'moved-notes';
