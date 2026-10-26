# Mini MapReduce — Word Count

## Objective
Implement a small, working MapReduce engine from scratch in Python. This implementation uses only the standard library, one input file per mapper task, concurrent threads, local combiners, and exactly two reducers. No Hadoop, Spark, or MapReduce framework is used.

Source repository: https://github.com/dr-ahmed-said/Big-Data-Analytics

Inspected upstream revision: `ec32eab0e8b69893d32c21cb6ff96ecb5df7b87c`.
The professor’s root README instructions and dataset are preserved. The root README now includes student ibrahim mohamed ahmed, ID 231006724.

## What is MapReduce?
MapReduce divides a counting job into small independent tasks, then gathers results by key. For word count, a key is a word and its value is a count. If two files contain `cat cat` and `CAT dog`, the final result is `cat 3` and `dog 1`.

## Five phases
1. **Input splits:** `input_splits` discovers sorted `.txt` files. Each file is one split. `expected_word_count.txt` is explicitly excluded, as it is a reference result, not an input document. The supplied folder contains five books.
2. **Map:** `map_words` emits `(word, 1)` for each word. Text is lowercase; punctuation, digits, and underscores are separators. Unicode word letters are supported. No stemming or stop-word removal occurs.
3. **Parallel execution:** `ThreadPoolExecutor` schedules one mapper task per split. Normally four workers bound concurrency. `--demonstrate` assigns one thread per split and uses a barrier so every task starts before any maps. This option is intended for small teaching datasets.
4. **Combine and partition:** `combine` sums duplicate words inside one split. `partition` computes `SHA-256(UTF-8 word) modulo 2`. The same word always goes to the same reducer, independent of Python's randomized built-in hash.
5. **Shuffle, sort, reduce:** Combined pairs are grouped by reducer, then by word. Each reducer sorts its keys and sums all values. The program writes two tab-separated output files, including empty outputs when appropriate.

## Implementation and important decisions
- `mapper`: reads one file, runs map/combine, partitions pairs, and records execution timestamps and thread name.
- `shuffle_sort_reduce`: groups values using a dictionary of lists, sorts words, and sums counts.
- `run`: coordinates mapper futures, routes pairs, runs the two logical reducers, and writes results.
- `read_counts`: reads reference/output files and rejects duplicate keys instead of silently overwriting them.
- `validate.py`: uses an independent character-scanning sequential counter to validate the supplied books. It also records every discrepancy with the professor's reference.
- Threads demonstrate concurrent scheduling and overlap file I/O. Python's GIL means this is not a claim of multicore CPU speedup. Reducers execute sequentially; only mappers are required to be concurrent.
- All data fits in memory. This is an educational engine, not a fault-tolerant distributed system. There is no automatic retry, external sort, or network transport.
- Output is sorted within each reducer. Concatenating the two files does not globally sort all words.

## Requirements and execution
Python 3.10 or newer; no packages to install. From the repository root:

```bash
python MapReduce-Assignment/mapreduce.py --demonstrate
python MapReduce-Assignment/validate.py
python -m unittest discover -s MapReduce-Assignment/tests -v
```

Custom folder:

```bash
python MapReduce-Assignment/mapreduce.py --input path/to/texts --output path/to/results --workers 4
```

Use a separate output directory. Use `--pattern 'book_*.txt'` if restricting filenames. Paths passed explicitly are relative to the current directory; default paths are anchored to the script. Missing input folders and nonpositive worker counts fail clearly. An empty input folder produces two empty reducer files.

For a valid external expected file, use `--expected path/to/expected.txt`. Strict validation fails on the supplied reference because it contains duplicate `reduce` entries. Use `validate.py` to inspect the supplied reference without losing information.

## Actual execution results
Executed on 26 October 2026:

| Measurement | Observed result |
|---|---:|
| Input splits | 5 |
| Mapper tasks in demonstration | 5 distinct threads |
| Total word occurrences | 235 |
| Unique words | 182 |
| Pairs after local combining | 215 |
| Reducers/output parts | 2 |
| Automated tests passed | 11 of 11 |
| Independent sequential reference | Exact match |
| Supplied expected result | Does not match; 40 affected keys |

`output/execution.json` records actual timestamps. All five tasks started before the first mapping began; the barrier makes this evidence of concurrent scheduling rather than relying on artificial sleeps. Timestamps and test durations change between runs; word-count outputs remain deterministic.

## Testing and validation
Tests cover multiple files, repeated words, capitalization, punctuation, empty files, an empty folder, excluded expected results, deterministic partitioning across interpreters, concurrent scheduling, invalid inputs, duplicate reference keys, dataset correctness, reducer membership, sorted keys, disjoint reducer outputs, and identical output with one versus four workers.

The first test execution revealed duplicate keys in the provided expected file. Inspection found further discrepancies. The pipeline was then independently validated; the supplied file was preserved rather than edited to force a match. For example:

| Word | Actual from books | Supplied reference |
|---|---:|---:|
| data | 2 | 1 |
| before | 2 | 1 |
| to | 1 | 4 |
| reduce | 2 | two rows: 2 and 1 |
| balanced | 0 | 1 |

See `output/validation.json` for all 40 differences and `output/tests.txt` for the actual successful test log. This discrepancy should be mentioned to the professor; the submission does not claim the provided expected output matches.

## Files
- `mapreduce.py`: engine and command-line interface.
- `validate.py`: independent count and expected-result audit.
- `tests/test_mapreduce.py`: 11 automated tests with temporary sample files.
- `output/part-r-00000.txt`, `output/part-r-00001.txt`: generated reducer outputs.
- `output/execution.json`: concurrency and execution evidence.
- `output/validation.json`: independent validation and expected-result differences.
- `output/tests.txt`: captured test run.
- `EXPLANATION.md`: Egyptian Arabic and English study guide.
- `.gitignore`: excludes Python bytecode.

## Conclusion
The implementation completes all five phases, produces deterministic two-reducer word counts, and passes independent validation. The professor's expected file contains discrepancies documented separately. Verified student details are included in the root README. Repository: https://github.com/shalaby10/Big-Data-Analytics
