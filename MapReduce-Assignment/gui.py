"""Desktop dashboard for the existing MapReduce engine; Python/Tk only."""
import csv
import os
from pathlib import Path
import queue
import subprocess
import sys
import threading
import time
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

from mapreduce import input_splits, partition, run

BASE = Path(__file__).resolve().parent
BG, PANEL, TEXT, MUTED, ACCENT = '#0b1220', '#141f32', '#eef4ff', '#93a6c3', '#56ddba'


class Dashboard(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title('MapReduce Studio | Word Count')
        self.geometry('1180x800')
        self.minsize(960, 700)
        self.configure(bg=BG)
        self.events = queue.Queue()
        self.counts = {}
        self.busy = False
        self.completed_output = None
        self.source = tk.StringVar(value=str(BASE.parent / 'datasets/text-files'))
        self.destination = tk.StringVar(value=str(BASE / 'gui-output'))
        self.search = tk.StringVar()
        self.reducer = tk.StringVar(value='All reducers')
        self.order = tk.StringVar(value='Count: high to low')
        self.workers = tk.IntVar(value=4)
        self.demo = tk.BooleanVar(value=True)
        self.status = tk.StringVar(value='Ready to run · Select your data, then start the pipeline.')
        self.stats = [tk.StringVar(value='—') for _ in range(4)]
        self.setup_style()
        self.build()
        self.search.trace_add('write', lambda *_: self.refresh_results())
        self.after(80, self.poll)
        self.protocol('WM_DELETE_WINDOW', self.close)

    def setup_style(self):
        style = ttk.Style(self)
        style.theme_use('clam')
        style.configure('.', font=('Segoe UI', 10), background=PANEL, foreground=TEXT)
        style.configure('TFrame', background=BG)
        style.configure('Card.TFrame', background=PANEL)
        style.configure('TLabel', background=BG, foreground=TEXT)
        style.configure('Muted.TLabel', foreground=MUTED)
        style.configure('TEntry', fieldbackground='#1d2b42', foreground=TEXT, padding=8)
        style.configure('TCombobox', fieldbackground='#1d2b42', foreground=TEXT, padding=5)
        style.map('TCombobox', fieldbackground=[('readonly', '#1d2b42')], foreground=[('readonly', TEXT)])
        style.configure('TButton', background='#253651', foreground=TEXT, padding=(14, 9), borderwidth=0)
        style.map('TButton', background=[('active', '#354b6c')])
        style.configure('Primary.TButton', background=ACCENT, foreground=BG, font=('Segoe UI', 11, 'bold'))
        style.map('Primary.TButton', background=[('active', '#8cebd1'), ('disabled', '#354b6c')])
        style.configure('TCheckbutton', background=BG, foreground=TEXT)
        style.map('TCheckbutton', background=[('active', BG)])
        style.configure('Treeview', background=PANEL, fieldbackground=PANEL, foreground=TEXT,
                        rowheight=30, borderwidth=0)
        style.configure('Treeview.Heading', background='#20304a', foreground=MUTED,
                        font=('Segoe UI', 10, 'bold'), padding=9)
        style.map('Treeview', background=[('selected', '#275f68')])
        style.configure('TNotebook', background=BG, borderwidth=0)
        style.configure('TNotebook.Tab', padding=(18, 10), background=PANEL, foreground=MUTED)
        style.map('TNotebook.Tab', background=[('selected', '#253651')], foreground=[('selected', TEXT)])
        style.configure('Horizontal.TProgressbar', background=ACCENT, troughcolor=PANEL, borderwidth=0)

    def build(self):
        page = ttk.Frame(self, padding=26)
        page.pack(fill='both', expand=True)
        head = ttk.Frame(page)
        head.pack(fill='x')
        ttk.Label(head, text='MAPREDUCE / STUDIO', foreground=ACCENT,
                  font=('Segoe UI', 10, 'bold')).pack(anchor='w')
        ttk.Label(head, text='Small splits. Clear results.', font=('Segoe UI', 27, 'bold')).pack(anchor='w', pady=(5, 0))
        ttk.Label(head, text='Concurrent word counting · 2 deterministic reducers · Python standard library',
                  style='Muted.TLabel').pack(anchor='w', pady=(5, 18))
        config = ttk.Frame(page)
        config.pack(fill='x')
        config.columnconfigure(1, weight=1)
        self.config_controls = []
        for row, (label, variable) in enumerate([('Input folder', self.source), ('Output folder', self.destination)]):
            ttk.Label(config, text=label, width=14).grid(row=row, column=0, sticky='w', pady=4)
            entry = ttk.Entry(config, textvariable=variable)
            entry.grid(row=row, column=1, sticky='ew', padx=(0, 10), pady=4)
            button = ttk.Button(config, text='Browse…', command=lambda v=variable: self.browse(v))
            button.grid(row=row, column=2)
            self.config_controls.extend([entry, button])
        actions = ttk.Frame(page)
        actions.pack(fill='x', pady=(12, 16))
        self.run_button = ttk.Button(actions, text='▶  Run pipeline', style='Primary.TButton', command=self.start)
        self.run_button.pack(side='left')
        ttk.Label(actions, text='Workers').pack(side='left', padx=(20, 6))
        workers = ttk.Spinbox(actions, from_=1, to=32, textvariable=self.workers, width=4)
        workers.pack(side='left')
        demo = ttk.Checkbutton(actions, text='Demonstrate concurrency', variable=self.demo)
        demo.pack(side='left', padx=12)
        self.config_controls.extend([workers, demo])
        ttk.Button(actions, text='Open output folder', command=self.open_output).pack(side='right')
        cards = ttk.Frame(page)
        cards.pack(fill='x', pady=(0, 16))
        for index, title in enumerate(['INPUT SPLITS', 'TOTAL WORDS', 'UNIQUE WORDS', 'REDUCERS']):
            cards.columnconfigure(index, weight=1)
            card = ttk.Frame(cards, style='Card.TFrame', padding=15)
            card.grid(row=0, column=index, sticky='ew', padx=(0 if index == 0 else 10, 0))
            tk.Label(card, text=title, bg=PANEL, fg=MUTED, font=('Segoe UI', 9, 'bold')).pack(anchor='w')
            tk.Label(card, textvariable=self.stats[index], bg=PANEL, fg=ACCENT,
                     font=('Segoe UI', 25, 'bold')).pack(anchor='w', pady=(5, 0))
        self.notebook = ttk.Notebook(page)
        self.notebook.pack(fill='both', expand=True)
        results, traces, activity = [ttk.Frame(self.notebook, padding=12) for _ in range(3)]
        for frame, title in [(results, 'Word counts'), (traces, 'Mapper execution'), (activity, 'Run summary')]:
            self.notebook.add(frame, text=title)
        filters = ttk.Frame(results)
        filters.pack(fill='x', pady=(0, 10))
        ttk.Label(filters, text='Find a word').pack(side='left', padx=(0, 8))
        ttk.Entry(filters, textvariable=self.search, width=22).pack(side='left')
        for var, options in [(self.reducer, ['All reducers', 'Reducer 0', 'Reducer 1']),
                             (self.order, ['Count: high to low', 'Word: A to Z'])]:
            box = ttk.Combobox(filters, textvariable=var, values=options, state='readonly', width=20)
            box.pack(side='left', padx=(10, 0))
            box.bind('<<ComboboxSelected>>', lambda _: self.refresh_results())
        ttk.Button(filters, text='Export CSV', command=self.export).pack(side='right')
        self.words = self.table(results, [('word', 'WORD', 420), ('count', 'COUNT', 130), ('reducer', 'REDUCER', 130)])
        self.traces = self.table(traces, [('split', 'INPUT SPLIT', 220), ('thread', 'THREAD', 140),
                                         ('tokens', 'WORDS', 80), ('pairs', 'COMBINED PAIRS', 130),
                                         ('start', 'START (ms)', 110), ('duration', 'DURATION (ms)', 130)])
        self.log = tk.Text(activity, bg=PANEL, fg=TEXT, relief='flat', wrap='word',
                           font=('Consolas', 11), padx=15, pady=15, state='disabled')
        self.log.pack(fill='both', expand=True)
        self.write_log('Ready. No pipeline has been executed in this window yet.\n\n'
                       'The expected_word_count.txt reference is excluded from input.\n'
                       'Demo mode uses one thread per split; workers apply in normal mode.\n'
                       'Use the CLI validation report to review supplied-reference discrepancies.')
        self.progress = ttk.Progressbar(page, mode='indeterminate')
        self.progress.pack(fill='x', pady=(14, 8))
        ttk.Label(page, textvariable=self.status, style='Muted.TLabel').pack(anchor='w')

    @staticmethod
    def table(parent, columns):
        frame = ttk.Frame(parent)
        frame.pack(fill='both', expand=True)
        tree = ttk.Treeview(frame, columns=[c[0] for c in columns], show='headings')
        for key, title, width in columns:
            tree.heading(key, text=title)
            tree.column(key, width=width, minwidth=70, anchor='w')
        scroll = ttk.Scrollbar(frame, orient='vertical', command=tree.yview)
        tree.configure(yscrollcommand=scroll.set)
        tree.pack(side='left', fill='both', expand=True)
        scroll.pack(side='right', fill='y')
        return tree

    def browse(self, variable):
        path = filedialog.askdirectory(parent=self, title='Choose folder')
        if path:
            variable.set(path)

    def write_log(self, text):
        self.log.configure(state='normal')
        self.log.delete('1.0', 'end')
        self.log.insert('end', text)
        self.log.configure(state='disabled')

    def start(self):
        if self.busy:
            return
        try:
            if not self.source.get().strip() or not self.destination.get().strip():
                raise ValueError('Select both input and output folders.')
            source, destination = Path(self.source.get()).resolve(), Path(self.destination.get()).resolve()
            splits = input_splits(source)
            workers = self.workers.get()
            if not 1 <= workers <= 32:
                raise ValueError('Choose 1–32 workers.')
            if source == destination:
                raise ValueError('Choose a different folder for output.')
            if self.demo.get() and len(splits) > 32:
                raise ValueError('Demo mode supports up to 32 files. Disable it for larger inputs.')
        except (ValueError, tk.TclError) as error:
            messagebox.showerror('Check configuration', str(error), parent=self)
            return
        if any((destination / n).exists() for n in ['part-r-00000.txt', 'part-r-00001.txt', 'execution.json']):
            if not messagebox.askyesno('Replace previous results?',
                                      'Generated result files already exist in this output folder. Replace them?', parent=self):
                return
        self.busy = True
        self.run_button.configure(state='disabled')
        for control in self.config_controls:
            control.configure(state='disabled')
        self.progress.start(12)
        self.status.set(f'Running all five phases · {len(splits)} input splits…')
        self.write_log('Pipeline running. Results will appear when all phases finish.')
        threading.Thread(target=self.execute, args=(source, destination, workers, self.demo.get()), daemon=True).start()

    def execute(self, source, destination, workers, demo):
        started = time.perf_counter()
        try:
            counts, report = run(source, destination, workers=workers, demonstrate=demo)
            self.events.put(('done', (counts, report, time.perf_counter() - started, destination)))
        except Exception as error:
            self.events.put(('error', str(error)))

    def poll(self):
        try:
            kind, payload = self.events.get_nowait()
        except queue.Empty:
            self.after(80, self.poll)
            return
        self.busy = False
        self.progress.stop()
        self.run_button.configure(state='normal')
        for control in self.config_controls:
            control.configure(state='normal')
        if kind == 'error':
            self.status.set('Run failed · Correct the configuration and try again.')
            self.write_log('ERROR\n' + payload)
            messagebox.showerror('Pipeline failed', payload, parent=self)
        else:
            self.counts, report, elapsed, self.completed_output = payload
            for variable, key in zip(self.stats, ['splits', 'total_words', 'unique_words', 'reducers']):
                variable.set(f'{report[key]:,}')
            self.refresh_results()
            self.traces.delete(*self.traces.get_children())
            traces = report['mapper_traces']
            origin = min((t['started'] for t in traces), default=0)
            for t in traces:
                self.traces.insert('', 'end', values=(t['split'], t['thread'], t['tokens'], t['combined_pairs'],
                                   f"{(t['started'] - origin)*1000:.3f}", f"{(t['finished']-t['started'])*1000:.3f}"))
            proof = 'confirmed' if report['all_tasks_started_before_first_map'] else 'not observed in this run'
            self.write_log(f'COMPLETED in {elapsed:.3f} seconds\n\n'
                           f'1. Input: {report["splits"]} files / splits\n'
                           '2. Map: lowercase words emitted as (word, 1)\n'
                           f'3. Concurrent mappers: all started before first map — {proof}\n'
                           '4. Combine + partition: SHA-256 modulo 2\n'
                           '5. Shuffle + sort + reduce: two output files written\n\n'
                           f'Output: {self.completed_output}\n\n'
                           'Completion indicates the pipeline ran successfully. It does not mean the\n'
                           'professor’s supplied expected counts match. Run validate.py for that audit.\n'
                           'Thread times include barrier waiting and file I/O; they are not CPU benchmarks.')
            self.status.set(f'Completed · {report["unique_words"]:,} unique words · {elapsed:.3f}s · Results saved')
        self.after(80, self.poll)

    def refresh_results(self):
        self.words.delete(*self.words.get_children())
        query = self.search.get().lower()
        selected = self.reducer.get()
        items = [(w, c) for w, c in self.counts.items() if query in w and
                 (selected == 'All reducers' or partition(w) == int(selected[-1]))]
        items.sort(key=(lambda pair: (-pair[1], pair[0])) if self.order.get().startswith('Count') else lambda pair: pair[0])
        for word, count in items:
            self.words.insert('', 'end', values=(word, count, f'Reducer {partition(word)}'))

    def export(self):
        if not self.counts:
            messagebox.showinfo('Export results', 'Run a pipeline with words before exporting.', parent=self)
            return
        path = filedialog.asksaveasfilename(parent=self, defaultextension='.csv',
                                           initialfile='word_counts.csv', filetypes=[('CSV file', '*.csv')])
        if path:
            try:
                with open(path, 'w', encoding='utf-8', newline='') as handle:
                    writer = csv.writer(handle)
                    writer.writerow(['word', 'count', 'reducer'])
                    writer.writerows((w, self.counts[w], partition(w)) for w in sorted(self.counts))
                self.status.set('CSV exported · All words included')
            except OSError as error:
                messagebox.showerror('Export failed', str(error), parent=self)

    def open_output(self):
        if self.completed_output is None:
            messagebox.showinfo('Output folder', 'Complete a pipeline run first.', parent=self)
            return
        try:
            if sys.platform == 'win32':
                os.startfile(self.completed_output)
            else:
                subprocess.Popen(['open' if sys.platform == 'darwin' else 'xdg-open', str(self.completed_output)])
        except OSError as error:
            messagebox.showerror('Cannot open folder', str(error), parent=self)

    def close(self):
        if not self.busy or messagebox.askyesno('Pipeline running', 'Close while processing? Output may be incomplete.', parent=self):
            self.destroy()


if __name__ == '__main__':
    Dashboard().mainloop()
