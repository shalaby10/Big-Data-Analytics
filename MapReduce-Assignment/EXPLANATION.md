# شرح المشروع بالمصري — English study guide

## الفكرة ببساطة
تخيّل عندك ٥ ملفات وعايز تعرف كل كلمة اتكررت كام مرة فيهم. بدل ما عامل واحد يقرأهم كلهم، بندّي كل ملف لمهمة اسمها Mapper. بعد كده بنجمّع نتائجهم علشان نطلع العدد النهائي لكل كلمة.

Imagine five text files and a goal: count every word across all files. Each mapper handles one file. We then merge their partial counts into final totals.

## مثال خطوة بخطوة
الملف الأول: `Cat cat`، والملف التاني: `CAT dog`.

| المرحلة / Phase | اللي بيحصل / What happens |
|---|---|
| Input splits | كل ملف شغلانة مستقلة. Each file is one independent split. |
| Map | بنحوّل الحروف لصغيرة ونطلع `(cat, 1)` لكل ظهور. Lowercase and emit one pair per occurrence. |
| Concurrent execution | مهام الملفات بتتجدول على threads مختلفة. File tasks are scheduled on different threads. |
| Combine | أول ملف يبقى `(cat, 2)` بدل زوجين. Locally sum repeated words. |
| Partition | كل أزواج `cat` تروح لنفس الـ reducer. Route each word consistently to one reducer. |
| Shuffle and sort | نجمع قيم `cat` زي `[2, 1]` ونرتّب المفاتيح. Group counts by word and sort keys. |
| Reduce | نجمع `2 + 1`، فالناتج `cat 3` و`dog 1`. Sum values for each key. |

الـ combine والـ partition مرحلتين جوه المرحلة الرابعة المطلوبة. والـ shuffle والـ sort والـ reduce خطوات جوه المرحلة الخامسة.

## أهم الدوال وليه استخدمناها
- `input_splits`: بتجيب ملفات الإدخال؛ بنستبعد ملف الإجابة علشان ما نعدّوش ضمن الكتب. Finds inputs while excluding the expected-answer file.
- `map_words`: بتحوّل النص لأزواج كلمة ورقم ١. Emits word/one pairs using a generator, so we don't create a full list of raw pairs.
- `combine`: بتقلّل عدد الأزواج اللي هننقلها للمرحلة اللي بعدها. Reduces intermediate traffic by summing locally.
- `partition`: بتستخدم SHA-256 وباقي القسمة على ٢. النتيجة صفر أو واحد، يعني رقم الـ reducer. A stable hash assigns each word to reducer 0 or 1.
- `shuffle_sort_reduce`: بتجمع قيم الكلمة من كل الملفات وتجمع أرقامها. Groups counts across files and sums them.
- `run`: المدير اللي بينسّق كل المراحل ويكتب الملفات. Coordinates the pipeline and writes outputs.
- `validate.py`: بيعدّ الكلمات بطريقة تانية مستقلة علشان نراجع النتيجة. Independently checks the result using a character scanner.

## أسئلة ممكن الدكتور يسألها
**ليه combine وreduce الاتنين بيجمعوا؟**
الـ combine بيجمع جوه ملف واحد. الـ reduce بيجمع نتائج نفس الكلمة من كل الملفات. الجمع ينفع هنا لأنه associative وcommutative؛ ترتيب وتجميع الأرقام مش بيغيّر الناتج.

The combiner sums within one split; the reducer sums across splits. Integer addition is associative and commutative, so local aggregation preserves the final result.

**ليه مش استخدمنا `hash(word)`؟**
لأنه ممكن يغيّر النتيجة بين مرات تشغيل بايثون. SHA-256 ثابت لنفس الكلمة.

Python's built-in string hash is randomized across runs. SHA-256 gives repeatable partition assignments.

**إزاي أثبتنا إن المهام اتجدولت مع بعض؟**
في وضع العرض، كل mapper بيسجّل وقت بدايته وبعدين يستنى عند barrier. محدش يبدأ العدّ غير لما كل المهام توصل. سجل التنفيذ أثبت وجود ٥ threads مختلفة وإن كل المهام بدأت قبل أول عملية mapping.

The demonstration barrier requires all five mapper threads to start before mapping proceeds. The trace records their names and times. This proves concurrent scheduling, not simultaneous CPU execution or a speedup.

**هل الـ reducer لازم يشتغل بالتوازي؟**
المطلوب هنا توازي الـ mappers. عندنا اتنين reducers منطقيين بيشتغلوا واحد بعد التاني، وكل واحد بيكتب ملفه.

Mapper concurrency is required. The two logical reducers run sequentially and each writes its own output file.

**ليه النتيجة مختلفة عن ملف الدكتور؟**
العدّ المستقل طابق برنامجنا: ٢٣٥ كلمة إجمالًا و١٨٢ كلمة مختلفة. ملف الإجابة فيه أخطاء زي تكرار `reduce` في سطرين، وكلمة `data` مكتوبة مرة رغم وجودها مرتين. سيبنا الملف زي ما هو ووثّقنا الاختلافات، فلازم نوضح ده للدكتور.

The independent counter matches all our counts. The supplied reference has duplicate keys and inconsistent counts; all 40 affected keys are documented without altering that file.

**هل ده نظام Big Data حقيقي؟**
ده نموذج تعليمي بيوضح الفكرة. البيانات هنا موجودة في الذاكرة وعلى جهاز واحد. النظام الكبير محتاج توزيع على أجهزة، تخزين وسيط، ومعالجة الأعطال.

This is an in-memory, single-machine teaching engine. Production systems additionally need distributed storage, fault tolerance, and scalable shuffling.
