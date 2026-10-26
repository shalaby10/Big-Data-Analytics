from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from mapreduce import input_splits, map_words, partition, read_counts, run
from validate import sequential_reference


class MapReduceTests(unittest.TestCase):
    def execute(self, files, demonstrate=False):
        with tempfile.TemporaryDirectory() as temp:
            source, output = Path(temp) / 'input', Path(temp) / 'output'
            source.mkdir()
            for name, content in files.items():
                (source / name).write_text(content, encoding='utf-8')
            counts, report = run(source, output, demonstrate=demonstrate)
            parts = sorted(output.glob('part-r-*.txt'))
            self.assertEqual(len(parts), 2)
            seen = set()
            for index, part in enumerate(parts):
                rows = read_counts(part)
                self.assertEqual(list(rows), sorted(rows))
                self.assertFalse(seen & rows.keys())
                self.assertTrue(all(partition(word) == index for word in rows))
                seen.update(rows)
            self.assertEqual(seen, counts.keys())
            return counts, report

    def test_multiple_files_repeated_words_case_and_empty(self):
        counts, report = self.execute({'a.txt': 'Hello hello WORLD!',
                                      'b.txt': 'world, HELLO. map-reduce', 'empty.txt': ''})
        self.assertEqual(counts, {'hello': 3, 'world': 2, 'map': 1, 'reduce': 1})
        self.assertEqual(report['splits'], 3)
        self.assertLess(sum(t['combined_pairs'] for t in report['mapper_traces']), 7)

    def test_empty_files(self):
        self.assertEqual(self.execute({'empty.txt': ''})[0], {})

    def test_empty_folder(self):
        self.assertEqual(self.execute({})[0], {})

    def test_expected_output_excluded(self):
        self.assertEqual(self.execute({'a.txt': 'cat', 'expected_word_count.txt': 'wrong 999',
                                       'README.md': 'wrong'})[0], {'cat': 1})

    def test_tokenization(self):
        self.assertEqual(list(map_words('CAT cat_42 café!')), [('cat', 1), ('cat', 1), ('café', 1)])

    def test_concurrent_scheduling(self):
        _, report = self.execute({f'{i}.txt': 'one two' for i in range(5)}, True)
        self.assertTrue(report['all_tasks_started_before_first_map'])
        self.assertEqual(len({t['thread'] for t in report['mapper_traces']}), 5)

    def test_deterministic_partition_across_interpreters(self):
        code = 'from mapreduce import partition; print(partition("hello"))'
        result = subprocess.check_output([sys.executable, '-c', code],
                                         cwd=Path(__file__).resolve().parents[1], text=True)
        self.assertEqual(int(result), partition('hello'))

    def test_invalid_input(self):
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaises(ValueError):
                input_splits(Path(temp) / 'missing')
            with self.assertRaises(ValueError):
                run(temp, temp)
            with self.assertRaises(ValueError):
                run(temp, Path(temp) / 'out', workers=0)

    def test_professor_dataset(self):
        source = Path(__file__).resolve().parents[2] / 'datasets/text-files'
        with tempfile.TemporaryDirectory() as output:
            counts, report = run(source, output)
            self.assertEqual(report['splits'], 5)
            self.assertEqual(counts, sequential_reference(source))
            self.assertEqual(sum(counts.values()), 235)
            self.assertEqual(len(counts), 182)

    def test_duplicate_expected_key_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'expected.txt'
            path.write_text('reduce 2\nreduce 1\n')
            with self.assertRaises(ValueError):
                read_counts(path)

    def test_repeat_run_outputs_identical(self):
        source = Path(__file__).resolve().parents[2] / 'datasets/text-files'
        with tempfile.TemporaryDirectory() as temp:
            a, b = Path(temp) / 'a', Path(temp) / 'b'
            run(source, a, workers=1)
            run(source, b, workers=4)
            for name in ('part-r-00000.txt', 'part-r-00001.txt'):
                self.assertEqual((a / name).read_bytes(), (b / name).read_bytes())


if __name__ == '__main__':
    unittest.main()
