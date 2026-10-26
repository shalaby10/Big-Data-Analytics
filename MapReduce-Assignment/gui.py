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
BG, PANEL, TEXT, MUTED, ACCENT = '#f3f5fa', '#ffffff', '#172338', '#78869c', '#6554e8'
NAV = '#151d32'


class Dashboard(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title('MapReduce Studio | Word Count')
        self.geometry(f'{min(1440, self.winfo_screenwidth()-60)}x{min(900, self.winfo_screenheight()-90)}')
        self.minsize(1100, 700)
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
        style.configure('.', font=('Segoe UI', 10), background=BG, foreground=TEXT)
        style.configure('TFrame', background=BG)
        style.configure('Card.TFrame', background=PANEL)
        style.configure('TLabel', background=BG, foreground=TEXT)
        style.configure('Muted.TLabel', foreground=MUTED)
        style.configure('TEntry', fieldbackground=PANEL, foreground=TEXT, padding=8, bordercolor='#dde3ee')
        style.configure('TCombobox', fieldbackground=PANEL, foreground=TEXT, padding=6, bordercolor='#dde3ee')
        style.map('TCombobox', fieldbackground=[('readonly', PANEL)], foreground=[('readonly', TEXT)])
        style.configure('TSpinbox', fieldbackground=PANEL, foreground=TEXT, padding=5)
        style.configure('TButton', background='#e9edf6', foreground=TEXT, padding=(14, 9), borderwidth=0)
        style.map('TButton', background=[('active', '#dce2f1')])
        style.configure('Primary.TButton', background=ACCENT, foreground='white', font=('Segoe UI', 11, 'bold'), padding=(22, 13))
        style.map('Primary.TButton', background=[('active', '#5141cc'), ('disabled', '#b4addb')])
        style.configure('TCheckbutton', background=NAV, foreground='#d2daeb')
        style.map('TCheckbutton', background=[('active', NAV)], foreground=[('disabled', '#8490a9')])
        style.configure('Treeview', background=PANEL, fieldbackground=PANEL, foreground=TEXT,
                        rowheight=36, borderwidth=0, font=('Segoe UI', 10))
        style.configure('Treeview.Heading', background='#edf0f7', foreground='#77849a',
                        font=('Segoe UI', 9, 'bold'), padding=12, relief='flat')
        style.map('Treeview', background=[('selected', '#e8e3ff')], foreground=[('selected', '#4d3bbb')])
        style.configure('TNotebook', background=BG, borderwidth=0)
        style.configure('TNotebook.Tab', padding=(20, 12), background=BG, foreground=MUTED, font=('Segoe UI', 10, 'bold'))
        style.map('TNotebook.Tab', background=[('selected', PANEL)], foreground=[('selected', ACCENT)])
        style.configure('Horizontal.TProgressbar', background=ACCENT, troughcolor='#e4e8f2', borderwidth=0, thickness=3)

    @staticmethod
    def label(parent, text, size=10, color=TEXT, bold=False, bg=PANEL, **kw):
        widget = tk.Label(parent, text=text, bg=bg, fg=color,
                          font=('Segoe UI', size, 'bold' if bold else 'normal'), **kw)
        return widget

    def build(self):
        sidebar = tk.Frame(self, bg=NAV, width=280)
        sidebar.pack(side='left', fill='y')
        sidebar.pack_propagate(False)
        inner = tk.Frame(sidebar, bg=NAV)
        inner.pack(fill='both', expand=True, padx=24, pady=28)
        self.label(inner, 'M / R', 24, '#b4a6ff', True, NAV).pack(anchor='w')
        self.label(inner, 'MapReduce Studio', 17, 'white', True, NAV).pack(anchor='w', pady=(10, 3))
        self.label(inner, 'WORD COUNT WORKSPACE', 8, '#8592ad', True, NAV).pack(anchor='w')
        tk.Frame(inner, bg='#30394f', height=1).pack(fill='x', pady=25)
        self.label(inner, 'PIPELINE CONFIGURATION', 9, '#a5b0c7', True, NAV).pack(anchor='w', pady=(0, 18))
        self.config_controls = []
        for title, variable, hint in [('Input directory', self.source, 'One text file = one mapper split'),
                                      ('Save results to', self.destination, 'Two reducer files + execution trace')]:
            self.label(inner, title, 10, '#e3e8f4', True, NAV).pack(anchor='w')
            entry = tk.Entry(inner, textvariable=variable, bg='#232d45', fg='white', insertbackground='white',
                             relief='flat', font=('Segoe UI', 9), disabledbackground='#232d45', disabledforeground='#8490a9')
            entry.pack(fill='x', ipady=10, pady=(8, 7))
            button = ttk.Button(inner, text='Choose folder', command=lambda v=variable: self.browse(v))
            button.pack(fill='x')
            self.label(inner, hint, 8, '#8492ad', bg=NAV).pack(anchor='w', pady=(7, 20))
            self.config_controls.extend([entry, button])
        self.label(inner, 'Worker threads', 10, '#e3e8f4', True, NAV).pack(anchor='w')
        workers = ttk.Spinbox(inner, from_=1, to=32, textvariable=self.workers, width=5)
        workers.pack(fill='x', pady=(8, 10))
        demo = ttk.Checkbutton(inner, text='Concurrency demonstration', variable=self.demo)
        demo.pack(anchor='w')
        self.label(inner, 'Demo: one thread per file (up to 32).', 8, '#8492ad', bg=NAV).pack(anchor='w', pady=(8, 0))
        self.config_controls.extend([workers, demo])
        footer = tk.Frame(inner, bg=NAV)
        footer.pack(side='bottom', fill='x', pady=(16, 0))
        self.label(footer, 'ibrahim mohamed ahmed', 10, '#dbe2f1', True, NAV).pack(anchor='w')
        self.label(footer, '231006724  /  Big Data Analytics', 8, '#8492ad', bg=NAV).pack(anchor='w', pady=(4, 0))

        page = ttk.Frame(self, padding=(28, 24))
        page.pack(fill='both', expand=True)
        top = ttk.Frame(page)
        top.pack(fill='x', pady=(0, 18))
        titles = ttk.Frame(top)
        titles.pack(side='left')
        ttk.Label(titles, text='WORKSPACE  /  WORD COUNT', foreground=MUTED, font=('Segoe UI', 9, 'bold')).pack(anchor='w')
        ttk.Label(titles, text='Pipeline overview', font=('Segoe UI', 25, 'bold')).pack(anchor='w', pady=(7, 4))
        ttk.Label(titles, text='Turn text files into clear, reproducible results.', style='Muted.TLabel').pack(anchor='w')
        self.run_button = ttk.Button(top, text='Run pipeline  →', style='Primary.TButton', command=self.start)
        self.run_button.pack(side='right', anchor='center')

        cards = ttk.Frame(page)
        cards.pack(fill='x', pady=(0, 18))
        for index, (title, hint, color) in enumerate([
                ('INPUT SPLITS', 'Independent text files', ACCENT),
                ('TOTAL WORDS', 'All word occurrences', '#168f85'),
                ('UNIQUE WORDS', 'Distinct normalized keys', '#c47b26'),
                ('REDUCERS', 'Deterministic partitions', '#4478cd')]):
            cards.columnconfigure(index, weight=1, uniform='metrics')
            card = tk.Frame(cards, bg=PANEL, highlightbackground='#e3e7f0', highlightthickness=1)
            card.grid(row=0, column=index, sticky='nsew', padx=(0 if index == 0 else 12, 0))
            inside = tk.Frame(card, bg=PANEL)
            inside.pack(fill='both', expand=True, padx=17, pady=15)
            self.label(inside, title, 8, MUTED, True).pack(anchor='w')
            tk.Label(inside, textvariable=self.stats[index], bg=PANEL, fg=color,
                     font=('Segoe UI', 27, 'bold')).pack(anchor='w', pady=(6, 1))
            self.label(inside, hint, 8, MUTED).pack(anchor='w')

        phases = tk.Frame(page, bg='#eae6fc')
        phases.pack(fill='x', pady=(0, 18))
        for i, text in enumerate(['01  Split', '02  Map', '03  Execute', '04  Partition', '05  Reduce']):
            phases.columnconfigure(i, weight=1)
            self.label(phases, text, 9, '#6556a5', True, '#eae6fc').grid(row=0, column=i, pady=12)

        self.notebook = ttk.Notebook(page)
        self.notebook.pack(fill='both', expand=True)
        overview, results, traces, activity = [ttk.Frame(self.notebook, padding=(0, 14)) for _ in range(4)]
        for frame, title in [(overview, 'Overview'), (results, 'Word counts'), (traces, 'Mapper activity'), (activity, 'Execution log')]:
            self.notebook.add(frame, text=title)
        overview.columnconfigure(0, weight=3)
        overview.columnconfigure(1, weight=2)
        overview.rowconfigure(0, weight=1)
        self.bar_canvas = tk.Canvas(overview, bg=PANEL, highlightthickness=1, highlightbackground='#e3e7f0')
        self.bar_canvas.grid(row=0, column=0, sticky='nsew', padx=(0, 14))
        self.reducer_canvas = tk.Canvas(overview, bg=PANEL, highlightthickness=1, highlightbackground='#e3e7f0')
        self.reducer_canvas.grid(row=0, column=1, sticky='nsew')
        self.bar_canvas.bind('<Configure>', lambda _: self.draw_charts())
        self.reducer_canvas.bind('<Configure>', lambda _: self.draw_charts())

        filters = ttk.Frame(results)
        filters.pack(fill='x', pady=(0, 12))
        ttk.Entry(filters, textvariable=self.search, width=19).pack(side='left')
        self.search.set('')
        for var, options, width in [(self.reducer, ['All reducers', 'Reducer 0', 'Reducer 1'], 14),
                                   (self.order, ['Count: high to low', 'Word: A to Z'], 18)]:
            box = ttk.Combobox(filters, textvariable=var, values=options, state='readonly', width=width)
            box.pack(side='left', padx=(8, 0))
            box.bind('<<ComboboxSelected>>', lambda _: self.refresh_results())
        ttk.Button(filters, text='Export CSV', command=self.export).pack(side='right')
        ttk.Label(results, text='Search by word • Filters affect this table; CSV includes every word.', style='Muted.TLabel').pack(anchor='w', pady=(0, 10))
        self.words = self.table(results, [('word', 'WORD', 300), ('count', 'COUNT', 110), ('reducer', 'PARTITION', 130)])
        self.traces = self.table(traces, [('split', 'INPUT SPLIT', 140), ('thread', 'THREAD', 100),
                                         ('tokens', 'WORDS', 70), ('pairs', 'LOCAL PAIRS', 95),
                                         ('start', 'START (ms)', 95), ('duration', 'DURATION (ms)', 115)])
        self.log = tk.Text(activity, bg=PANEL, fg=TEXT, relief='flat', wrap='word',
                           font=('Consolas', 10), padx=20, pady=20, state='disabled')
        self.log.pack(fill='both', expand=True)
        self.write_log('READY TO RUN\n\nChoose the input and output folders in the left panel, then click Run pipeline.\n\n'
                       'The expected_word_count.txt reference is excluded from input.\n'
                       'Demo mode uses one thread per file; worker count applies in normal mode.')
        bottom = ttk.Frame(page)
        bottom.pack(fill='x', pady=(14, 0))
        ttk.Label(bottom, textvariable=self.status, style='Muted.TLabel', wraplength=590).pack(side='left')
        ttk.Button(bottom, text='Open output folder ↗', command=self.open_output).pack(side='right')
        self.progress = ttk.Progressbar(page, mode='indeterminate')
        self.progress.pack(fill='x', pady=(10, 0))
        self.draw_charts()

    def draw_charts(self):
        if not hasattr(self, 'bar_canvas') or not hasattr(self, 'reducer_canvas'):
            return
        bar, reducer = self.bar_canvas, self.reducer_canvas
        for canvas in (bar, reducer):
            canvas.delete('all')
        width, height = max(bar.winfo_width(), 280), max(bar.winfo_height(), 220)
        bar.create_text(22, 25, text='Most frequent words', anchor='w', fill=TEXT, font=('Segoe UI', 13, 'bold'))
        bar.create_text(22, 49, text='Top 8 words · count across all input files', anchor='w', fill=MUTED, font=('Segoe UI', 9))
        reducer.create_text(22, 25, text='Reducer distribution', anchor='w', fill=TEXT, font=('Segoe UI', 13, 'bold'))
        reducer.create_text(22, 49, text='Word occurrences assigned to each reducer', anchor='w', fill=MUTED, font=('Segoe UI', 9))
        if not self.counts:
            bar.create_text(width/2, height/2, text='No words found in this run' if self.completed_output else 'Your word insights will appear here', fill=MUTED, font=('Segoe UI', 11))
            reducer.create_text(22, 105, text='No words to distribute' if self.completed_output else 'No run yet', anchor='w', fill=MUTED, font=('Segoe UI', 11))
            return
        items = sorted(self.counts.items(), key=lambda x: (-x[1], x[0]))[:8]
        row = min(36, max(18, (height-90)/max(1, len(items))))
        maximum = max(c for _, c in items)
        for index, (word, count) in enumerate(items):
            y = 82 + index*row
            bar.create_text(22, y, text=word[:16], anchor='w', fill=TEXT, font=('Segoe UI', 10))
            left, right = 140, width-45
            bar.create_rectangle(left, y-6, right, y+6, fill='#f0edfc', outline='')
            bar.create_rectangle(left, y-6, left+(right-left)*count/maximum, y+6, fill=ACCENT if index==0 else '#b4a9f5', outline='')
            bar.create_text(width-18, y, text=str(count), anchor='e', fill=TEXT, font=('Segoe UI', 10, 'bold'))
        rw = max(reducer.winfo_width(), 250)
        total = sum(self.counts.values())
        for number, color in [(0, ACCENT), (1, '#26a69a')]:
            count = sum(c for w, c in self.counts.items() if partition(w)==number)
            y = 92+number*100
            reducer.create_text(22, y, text=f'Reducer {number}', anchor='w', fill=TEXT, font=('Segoe UI', 11, 'bold'))
            reducer.create_text(rw-22, y, text=f'{count:,} words', anchor='e', fill=color, font=('Segoe UI', 11, 'bold'))
            reducer.create_rectangle(22, y+20, rw-22, y+30, fill='#edf0f7', outline='')
            reducer.create_rectangle(22, y+20, 22+(rw-44)*count/total, y+30, fill=color, outline='')
            reducer.create_text(22, y+49, text=f'{count/total:.1%} of all occurrences', anchor='w', fill=MUTED, font=('Segoe UI', 9))

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
        tree.tag_configure('even', background='#ffffff')
        tree.tag_configure('odd', background='#f7f8fc')
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
            self.draw_charts()
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
        for index, (word, count) in enumerate(items):
            self.words.insert('', 'end', values=(word, count, f'Reducer {partition(word)}'), tags=('even' if index % 2 == 0 else 'odd',))

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
    if sys.platform == 'win32':
        try:
            import ctypes
            ctypes.windll.shcore.SetProcessDpiAwareness(1)
        except (AttributeError, OSError):
            pass
    Dashboard().mainloop()
