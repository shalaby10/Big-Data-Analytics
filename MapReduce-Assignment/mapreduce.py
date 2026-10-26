"""Five-phase word count using only Python's standard library (Python 3.10+)."""
import argparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import re
import threading
import time

REDUCERS = 2


def input_splits(folder, pattern='*.txt'):
    """One regular file per split; expected results are never input."""
    folder = Path(folder)
    if not folder.is_dir():
        raise ValueError(f'Input directory does not exist: {folder}')
    return sorted(p for p in folder.glob(pattern)
                  if p.is_file() and p.name != 'expected_word_count.txt')


def map_words(text):
    """Punctuation separates words; case is normalized; digits are excluded."""
    for word in re.findall(r'[^\W\d_]+', text.lower(), flags=re.UNICODE):
        yield word, 1


def combine(pairs):
    counts = defaultdict(int)
    for word, value in pairs:
        counts[word] += value
    return dict(counts)


def partition(word):
    """Stable across processes/runs, unlike Python's randomized hash()."""
    return int.from_bytes(hashlib.sha256(word.encode('utf-8')).digest(), 'big') % REDUCERS


def mapper(path, gate=None):
    started = time.perf_counter()
    thread = threading.current_thread().name
    # In demonstration mode every mapper reaches this gate before any maps.
    # This makes concurrent scheduling observable even for tiny input files.
    if gate is not None:
        gate.wait(timeout=30)
    map_started = time.perf_counter()
    counts = combine(map_words(path.read_text(encoding='utf-8')))
    buckets = [[], []]
    for word, count in counts.items():
        buckets[partition(word)].append((word, count))
    return buckets, {'split': path.name, 'thread': thread,
                     'started': started, 'map_started': map_started,
                     'finished': time.perf_counter(),
                     'tokens': sum(counts.values()), 'combined_pairs': len(counts)}


def shuffle_sort_reduce(pairs):
    grouped = defaultdict(list)
    for word, count in pairs:
        grouped[word].append(count)
    return [(word, sum(grouped[word])) for word in sorted(grouped)]


def run(input_dir, output_dir, pattern='*.txt', workers=4, demonstrate=False):
    if workers < 1:
        raise ValueError('workers must be at least 1')
    splits = input_splits(input_dir, pattern)
    output_dir = Path(output_dir)
    if output_dir.resolve() == Path(input_dir).resolve():
        raise ValueError('Output directory must differ from input directory')
    # One worker per split only in the optional, small-dataset demonstration.
    gate = threading.Barrier(len(splits)) if demonstrate and splits else None
    size = len(splits) if gate else workers
    with ThreadPoolExecutor(max_workers=max(1, size), thread_name_prefix='mapper') as pool:
        futures = [pool.submit(mapper, path, gate) for path in splits]
        results = [future.result() for future in futures]
    intermediate = [[], []]
    traces = []
    for buckets, trace in results:
        traces.append(trace)
        for index in range(REDUCERS):
            intermediate[index].extend(buckets[index])
    output_dir.mkdir(parents=True, exist_ok=True)
    final = {}
    for index in range(REDUCERS):
        reduced = shuffle_sort_reduce(intermediate[index])
        final.update(reduced)
        (output_dir / f'part-r-{index:05d}.txt').write_text(
            ''.join(f'{word}\t{count}\n' for word, count in reduced), encoding='utf-8')
    report = {'splits': len(splits), 'reducers': REDUCERS,
              'total_words': sum(final.values()), 'unique_words': len(final),
              'demonstration': demonstrate,
              'all_tasks_started_before_first_map': bool(traces) and
              max(t['started'] for t in traces) <= min(t['map_started'] for t in traces),
              'mapper_traces': traces}
    (output_dir / 'execution.json').write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    return final, report


def read_counts(path):
    counts = {}
    for line in Path(path).read_text(encoding='utf-8').splitlines():
        if line.strip():
            word, count = line.split()
            if word in counts:
                raise ValueError(f'Duplicate expected key: {word}')
            counts[word] = int(count)
    return counts


def main():
    base = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, default=base.parent / 'datasets/text-files')
    parser.add_argument('--output', type=Path, default=base / 'output')
    parser.add_argument('--pattern', default='*.txt')
    parser.add_argument('--workers', type=int, default=4)
    parser.add_argument('--demonstrate', action='store_true')
    parser.add_argument('--expected', type=Path)
    args = parser.parse_args()
    counts, report = run(args.input, args.output, args.pattern, args.workers, args.demonstrate)
    print(json.dumps(report, indent=2))
    if args.expected:
        expected = read_counts(args.expected)
        if counts != expected:
            differences = {w: {'actual': counts.get(w), 'expected': expected.get(w)}
                           for w in sorted(counts.keys() | expected.keys())
                           if counts.get(w) != expected.get(w)}
            print('VALIDATION FAILED:', json.dumps(differences, indent=2))
            raise SystemExit(1)
        print(f'VALIDATION PASSED: all {len(counts)} word counts match expected results.')


if __name__ == '__main__':
    main()
