# Training the compact tagger

The runtime tagger is a sparse linear-chain model with Viterbi inference. Both
training and inference use only the Python standard library.

Regenerate the bundled starter model:

```bash
python training/train_compact_tagger.py
```

Evaluate it on the small, deliberately unseen smoke corpus:

```bash
python training/evaluate_compact_tagger.py
```

The starter corpus contains synthetic public place names. It establishes a
reproducible model artifact and exercises the hybrid parser, but it must not be
presented as nationwide quality evidence.

A production training release should:

1. convert reviewed address rows into offset-preserving token labels;
2. group all variations of one canonical address in a single split;
3. reserve a manually reviewed gold set;
4. generate punctuation, abbreviation, deletion, reordering, and typo noise;
5. report token accuracy, full-sequence accuracy, and field exact match;
6. inspect failures by source system and region;
7. document the right to redistribute both examples and the derived model.
