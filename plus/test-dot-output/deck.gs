/**
 * Google Apps Script — Physical Merged-Index Maintenance deck builder.
 *
 * Usage:
 *   1. Create a Drive folder named exactly FOLDER_NAME below.
 *   2. Upload these 4 PNGs into it (renamed):
 *        q12_before-pipeline.png
 *        q12_physical-maintenance.png
 *        q3ol_physical-maintenance-0.png
 *        q3ol_physical-maintenance-1.png
 *   3. Paste this whole file into a new Apps Script project (script.google.com).
 *   4. Run `buildDeck`. Authorize Drive + Slides scopes when prompted.
 *   5. Open the URL printed in the execution log.
 */

const FOLDER_NAME = 'merged-index-deck-assets';
const DECK_TITLE  = 'Physical Merged-Index Maintenance';

// 16:9 layout (points)
const W = 960, H = 540;
const TITLE_BOX = {l: 40,  t: 30,  w: 880, h: 60};
const BODY_FULL = {l: 40,  t: 110, w: 880, h: 380};
const BODY_LEFT = {l: 40,  t: 110, w: 540, h: 380};
const IMG_RIGHT = {l: 600, t: 110, w: 320, h: 380};

const MONO  = 'Courier New';
const SERIF = 'Arial';

// ─── Entry point ───────────────────────────────────────────────────────────

function buildDeck() {
  const folder = DriveApp.getFoldersByName(FOLDER_NAME).next();
  const img = name => folder.getFilesByName(name).next().getBlob();

  const deck = SlidesApp.create(DECK_TITLE);
  // Drop the default blank slide.
  deck.getSlides()[0].remove();

  slideTitle(deck);
  slideRecap(deck, img('q12_before-pipeline.png'));
  slidePhysicalClaim(deck);
  slideFormalModel(deck);
  slideBtree(deck);
  slideLsmCompaction(deck);
  slideLsmVariants(deck);
  slideTradeoffs(deck);
  slideQ12(deck, img('q12_physical-maintenance.png'));
  slideQ3ol(deck,
    img('q3ol_physical-maintenance-0.png'),
    img('q3ol_physical-maintenance-1.png'));
  slideOpenQuestions(deck);

  Logger.log('Deck URL: ' + deck.getUrl());
}

// ─── Helpers ───────────────────────────────────────────────────────────────

function newSlide(deck) {
  return deck.appendSlide(SlidesApp.PredefinedLayout.BLANK);
}

function addTitle(slide, text) {
  const box = slide.insertTextBox(text,
      TITLE_BOX.l, TITLE_BOX.t, TITLE_BOX.w, TITLE_BOX.h);
  const tr = box.getText();
  tr.getTextStyle()
    .setFontFamily(SERIF)
    .setFontSize(32)
    .setBold(true);
  return box;
}

function addMono(slide, text, box) {
  box = box || BODY_FULL;
  const shape = slide.insertTextBox(text, box.l, box.t, box.w, box.h);
  shape.getText().getTextStyle()
    .setFontFamily(MONO)
    .setFontSize(13);
  return shape;
}

function addProse(slide, text, box) {
  box = box || BODY_FULL;
  const shape = slide.insertTextBox(text, box.l, box.t, box.w, box.h);
  shape.getText().getTextStyle()
    .setFontFamily(SERIF)
    .setFontSize(18);
  return shape;
}

function addImage(slide, blob, box) {
  return slide.insertImage(blob, box.l, box.t, box.w, box.h);
}

function setNotes(slide, text) {
  slide.getNotesPage().getSpeakerNotesShape().getText().setText(text);
}

// ─── Slides ────────────────────────────────────────────────────────────────

function slideTitle(deck) {
  const slide = newSlide(deck);
  const t = slide.insertTextBox(
      'Physical Execution of\nMerged Index Maintenance',
      40, 160, 880, 160);
  t.getText().getTextStyle()
    .setFontFamily(SERIF).setFontSize(40).setBold(true);

  const sub = slide.insertTextBox('Advisor Update', 40, 340, 880, 40);
  sub.getText().getTextStyle()
    .setFontFamily(SERIF).setFontSize(20).setForegroundColor('#666666');

  setNotes(slide,
    'One-line pitch: the maintenance plan is a single key-range scan over ' +
    'the same B-tree that holds the query data. No separate materialized ' +
    'view, no extra I/O.');
}

function slideRecap(deck, beforeImg) {
  const slide = newSlide(deck);
  addTitle(slide, 'Quick Recap');

  addProse(slide,
    '• Order-based plan → pipelines (bounded by sorts)\n\n' +
    '• Each boundary sort → merged index (B-tree, multi-table)\n\n' +
    '• The whole pipeline collapses to one scan',
    BODY_LEFT);

  addImage(slide, beforeImg, IMG_RIGHT);

  setNotes(slide,
    'From the Mar 27 update — feel free to skip if you remember. The image ' +
    'on the right is the traditional Q12 plan: sort, merge join, sorted ' +
    'aggregate. That whole stack becomes one merged-index scan.');
}

function slidePhysicalClaim(deck) {
  const slide = newSlide(deck);
  addTitle(slide, 'The Physical Claim');

  addMono(slide,
    '       δ row at key k\n' +
    '            │\n' +
    '            ▼\n' +
    '    ┌───────────────┐\n' +
    '    │ merged B-tree │   ← one seek, one forward pass\n' +
    '    │  R  S  T ...  │     all join partners co-located\n' +
    '    └───────────────┘\n\n' +
    '   Maintenance = single key-range scan over the same B-tree.');

  setNotes(slide,
    'Traditional IVM needs separate I/O into each materialized view: one ' +
    'lookup into a stored delta, another into each MV. Here, every leaf scan ' +
    'in the maintenance plan is a range scan over the same key range of the ' +
    'same B-tree.');
}

function slideFormalModel(deck) {
  const slide = newSlide(deck);
  addTitle(slide, 'Formal Model');

  addMono(slide,
    '   pending row  =  (r_id, s_id, t_id, ...)\n' +
    '                    └──── one ID per source ────┘\n\n\n' +
    '   cache size   =  |δR| + |δS| + |δT| + ...   (sum, not product)\n' +
    '                          ↑\n' +
    '                   does NOT blow up with join fanout');

  setNotes(slide,
    'A pipeline output row is identified by a tuple of source record IDs. ' +
    'A row is pending propagation if any source ID belongs to a record that ' +
    'has not yet been joined and written. The delta-key cache stores changed ' +
    'source records, not output tuples — its size grows linearly with deltas, ' +
    'not multiplicatively with join fanout.');
}

function slideBtree(deck) {
  const slide = newSlide(deck);
  addTitle(slide, 'B-tree Execution (on-demand)');

  addMono(slide,
    '   updates ──► δ-key cache (RAM)\n' +
    '                    │\n' +
    '            trigger │ range scan\n' +
    '                    ▼\n' +
    '               ┌─────────┐\n' +
    '               │ B-tree  │\n' +
    '               └─────────┘\n' +
    '                    │\n' +
    '            buffer records at key k\n' +
    '            → cartesian product\n' +
    '            → emit, clear, next key\n\n' +
    '   Cost ∝ distinct delta keys, not index size.');

  setNotes(slide,
    'Delta keys live in an in-memory cache as updates arrive. Maintenance is ' +
    'triggered explicitly: scan the B-tree over the cached key range, buffer ' +
    'records at each key, compute the local cartesian product, emit, clear ' +
    'the cache entry. Cost is proportional to the number of distinct delta ' +
    'keys.');
}

function slideLsmCompaction(deck) {
  const slide = newSlide(deck);
  addTitle(slide, 'LSM: Compaction IS Maintenance');

  addMono(slide,
    '   MemTable (new δ) ═╗\n' +
    '                     ╠═► compaction merge ═► SSTable\n' +
    '   SSTable (old)   ══╝       │\n' +
    '                              └─ key-range buffering\n' +
    '                                 cartesian product\n' +
    '                                 = the maintenance plan');

  setNotes(slide,
    'In an LSM tree, unpropagated records naturally sit in the MemTable ' +
    'because they were just written. Compaction merges the upper level with ' +
    'the lower level, which is exactly the key-range buffering and cartesian ' +
    'product that the maintenance plan describes. You get maintenance for ' +
    'free as a side effect of normal compaction.');
}

function slideLsmVariants(deck) {
  const slide = newSlide(deck);
  addTitle(slide, 'LSM Variants: Bookkeeping Cost');

  addMono(slide,
    '   2-level         │ level = propagation status   (free)\n\n' +
    '   Leveled multi   │ 1 propagation bit per record\n\n' +
    '   Tiered          │ full level counter   (messy within tier)');

  setNotes(slide,
    'In a two-level LSM, level membership encodes propagation status — free. ' +
    'In leveled multi-level, after the first compaction propagated and ' +
    'unpropagated records mingle within a level, so you need one bit. ' +
    'Tiered LSMs have multiple SSTables of different ages within a level, so ' +
    'you need a full level counter and have to keep it correct across ' +
    'within-tier merges.');
}

function slideTradeoffs(deck) {
  const slide = newSlide(deck);
  addTitle(slide, 'On-demand vs Compaction');

  addMono(slide,
    '                    on-demand        compaction\n' +
    '   ───────────────────────────────────────────────\n' +
    '   seek cost        1 per δ key      amortized\n' +
    '   update copy      yes (cache)      no\n' +
    '   freshness        immediate        batched\n' +
    '   scheduling       explicit         free ride');

  setNotes(slide,
    'On-demand pays a seek per delta key but propagates immediately. ' +
    'Compaction folds many deltas into a single sequential pass and avoids ' +
    'the separate cache, but only at the next compaction. Trade-off is ' +
    'freshness vs amortized I/O.');
}

function slideQ12(deck, physImg) {
  const slide = newSlide(deck);
  addTitle(slide, 'Example: Q12 (one pipeline)');

  addMono(slide,
    '  LINEITEM δ at orderkey k\n' +
    '         │\n' +
    '         ▼\n' +
    '  MI(ORDERS, LINEITEM\n' +
    '     by orderkey)\n' +
    '         │\n' +
    '   range scan at k\n' +
    '         │\n' +
    '   ≈ 1 ORDERS + ~4 LINEITEM\n' +
    '   (~1–2 pages)',
    BODY_LEFT);

  addImage(slide, physImg, IMG_RIGHT);

  setNotes(slide,
    'Q12 has one pipeline keyed by orderkey. Inserting one LINEITEM row ' +
    'requires a single range scan at that key. Traditional IVM would need ' +
    'roughly 3–5 random I/Os: one into a stored LINEITEM delta, one into an ' +
    'ORDERS materialized view, plus a write to a separate delta result table.');
}

function slideQ3ol(deck, innerImg, outerImg) {
  const slide = newSlide(deck);
  addTitle(slide, 'Example: Q3-OL (nested cascade)');

  addMono(slide,
    ' LINEITEM δ (orderkey k, custkey c)\n' +
    '        │\n' +
    '        ▼\n' +
    ' inner MI (ORDERS+LINEITEM by orderkey)\n' +
    '        │  ← 1 update, FK→PK, factor=1\n' +
    '        ▼\n' +
    ' outer MI (inner_view+CUSTOMER by custkey)\n' +
    '        │  ← 1 update, FK→PK, factor=1\n' +
    '        ▼\n' +
    ' (no final view — scanned at query time)',
    BODY_LEFT);

  // Stack the two physical-maintenance images on the right.
  const halfH = Math.floor(IMG_RIGHT.h / 2) - 8;
  addImage(slide, innerImg, {
    l: IMG_RIGHT.l, t: IMG_RIGHT.t,
    w: IMG_RIGHT.w, h: halfH
  });
  addImage(slide, outerImg, {
    l: IMG_RIGHT.l, t: IMG_RIGHT.t + halfH + 16,
    w: IMG_RIGHT.w, h: halfH
  });

  setNotes(slide,
    'Each level of the cascade is its own range scan over its own B-tree. ' +
    'The inner MI is read at orderkey k; the resulting view-row change is ' +
    'then propagated to the outer MI at custkey c. Both steps stay 1-to-1 ' +
    'because each join is FK→PK.');
}

function slideOpenQuestions(deck) {
  const slide = newSlide(deck);
  addTitle(slide, 'Open Questions');

  addMono(slide,
    '   Q1  per-update page count          analytical    HIGH\n' +
    '   Q2  delta amplification (1-to-1)   analytical    HIGH\n' +
    '   Q3  nested cascade cost            analytical    HIGH\n' +
    '   Q4  aggregation: rescan vs state   formalize     MED\n' +
    '   Q5  no-final-view savings          count writes  MED\n' +
    '   Q6  LSM batch connection           conceptual    MED\n\n' +
    '   Q1–Q3: enough for the efficiency story.\n' +
    '   Q4–Q6: add depth.');

  setNotes(slide,
    'Q1–Q3 are tractable from TPC-H statistics and PK/FK declarations — no ' +
    'implementation needed. The interesting edge case for Q2 is many-to-many ' +
    'joins like PARTSUPP, where the 1-to-1 property breaks. Q6 connects the ' +
    'maintenance plan to LSM compaction cost models from the storage engine ' +
    'literature.');
}
