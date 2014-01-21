lines = load 'data.tsv' as (line:chararray);

words = foreach lines generate flatten(TOKENIZE(line)) as word;

nested = foreach (group words by word) {
           silly = distinct words.word; -- nested proj
           generate
             group as word,
             flatten(silly) as uniq;
         };

rmf nested-test
store nested into 'nested-test';
