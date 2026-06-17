# Zero-Trust Intelligent Streaming Data Pipeline

A production-grade, event-driven data streaming pipeline built on GCP. It intercepts real-time chat/telemetry streams, redacts sensitive PII (SSNs, Credit Cards) on the fly, and uses Generative AI to categorize unstructured message intent before committing records securely to BigQuery analytics.

## 🏗️ Architecture Topography

[User Ingestion Stream] ──> [Cloud Pub/Sub]
│
▼
[Cloud Dataflow Engine]
(Apache Beam Pipeline)
│
┌──────────────────────┴──────────────────────┐
▼                                             ▼
[Sensitive Data Protection]                    [Vertex AI Engine]
(Inline PII Identification)                 (Gemini Intent Analysis)
│                                             │
▼                                             ▼
┌───────────────┐                             ┌───────────────┐
│ Quarantine DLQ│                             │  Production   │
└───────┬───────┘                             └───────┬───────┘
▼                                             ▼
[BQ Security Table]                           [BQ Analytics Table]

## 🛠️ Core Engineering Highlights
* **Inline PII Interception:** Leverages the Sensitive Data Protection API to run stateless, sub-second token replacement rules for automated masking.
* **Semantic Inference Layer:** Integrates Vertex AI (Gemini) inside structured processing worker nodes to determine business interaction categories.
* **Fault-Tolerant Split-Routing:** Employs explicit branching outputs (`with_outputs`) to route raw malicious structural data leaks directly to a quarantine audit table while allowing safe traffic to stream uninterrupted.

## 🚀 Deployment Instructions
1. Run `pip install -r requirements.txt` to align execution libraries.
2. Execute the Dataflow controller mapping targeting your active project instance:
   ```bash
   python pipeline.py --project your-project-id --runner DataflowRunner --region us-central1

3. Boot the stream simulator engine to drive live traffic:
python mock_stream.py

---

### 📤 Quick Command Sequence to Push to GitHub

Once you have saved these files inside your local project folder, push them cleanly using your terminal with these commands:

```bash
# Stage all files to your git cache
git add zero-trust-stream-pipeline/

# Commit the new pipeline layer
git commit -m "feat: Add Zero-Trust Intelligent Streaming pipeline framework files"

# Push securely to your remote main branch
git push origin main