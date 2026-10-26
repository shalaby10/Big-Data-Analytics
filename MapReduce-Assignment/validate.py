"""Independent reference count and transparent comparison to supplied results."""
from collections import Counter
import json
from pathlib import Path
from mapreduce import run


def sequential_reference(folder):
    # Separate tokenizer implementation: scan characters rather than use regex.
    counts = Counter()
    for path in sorted(Path(folder).glob('book_*.txt')):
        word = ''
        for char in path.read_text(encoding='utf-8').lower() + ' ':
            if char.isalpha():
                word += char
            elif word:
                counts[word] += 1
                word = ''
    return dict(counts)


def validate():
    base = Path(__file__).resolve().parent
    source = base.parent / 'datasets/text-files'
    actual, _ = run(source, base / 'output', demonstrate=True)
    reference = sequential_reference(source)
    assert actual == reference, 'Pipeline differs from independent sequential count'
    entries = {}
    for line in (source / 'expected_word_count.txt').read_text().splitlines():
        word, count = line.split()
        entries.setdefault(word, []).append(int(count))
    differences = {word: {'actual': actual.get(word, 0), 'supplied_entries': entries.get(word, [])}
                   for word in sorted(actual.keys() | entries.keys())
                   if entries.get(word) != [actual.get(word, 0)]}
    report = {'independent_reference_matches': True,
              'total_words': sum(actual.values()), 'unique_words': len(actual),
              'professor_expected_matches': not differences,
              'duplicate_expected_keys': {w: v for w, v in entries.items() if len(v) > 1},
              'differences': differences}
    (base / 'output/validation.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    validate()
