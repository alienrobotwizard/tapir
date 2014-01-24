input_data = load 'data/nestedfilter.tsv' as (source:chararray, docs:bag{t:tuple(text:chararray)});

filtered = foreach input_data {
             dogs = filter docs by text == 'dogs';
             generate
               flatten(dogs);
           };

store filtered into 'data/nested-filter-test-out';
