# Presenter Script — `continuum-ai-value.pptx` — United Airlines, 5-minute version

**Audience:** United Airlines — existing Solace customer; NOC/ops and technology leaders
**Run time:** 5 minutes spoken, tight. Timings per slide are hard budgets.
**Context to know:** United has already seen a Continuum + Solace Agent Mesh demo (Aug 2026); their leadership flagged the **NOC use case** as the cleanest path to an enterprise pattern. United also runs ConnectionSaver — they already believe in saving connections; the pitch is *how much faster and broader* that instinct can run.
**Hard rules:** the product is **Continuum** — never name the underlying engine. Say **"Solace Platform"**, never "PubSub+". Don't speed up to fit — cut, don't rush.

*Stage directions in [brackets]. **Bold** = the beats that must survive any compression. "→" is the transition, spoken while advancing.*

---

## SLIDE 1 — "Your AI Is Only as Good as What It Knows Right Now" (0:00–1:00)

[No warm-up. First sentence lands the whole premise:]

You're putting AI to work on real operational decisions — and at United's scale, an operational decision is rarely abstract: it's a specific airplane, a specific crew, and a few hundred specific passengers, right now.

Here's the problem worth five minutes of your day: **most AI acts on data that's minutes or hours old, scattered across systems.** So it makes the right call on old facts — the connection that *was* makeable. It's too slow — the answer arrives after the door closes. Your people don't fully trust it, because they can't tell if it's current. And it's expensive — every question re-fetches and re-computes the same answer, in tokens and in load on your systems of record.

**Stale context is the number one reason an agent acts wrong and sounds certain doing it.** In an IROPS event, that's not a quality issue — that's a trending-on-social-media issue.

→ "You already own the fix's hardest ingredient. Here's the missing piece."

---

## SLIDE 2 — "What Continuum Actually Does" (1:00–2:15)

[Walk the diagram left to right, briskly:]

Your Solace event mesh already carries the live truth of the operation — **every event, live**: flight status, itineraries, seats, crew, gates. That's the left of this picture, and it's already running at United.

What the mesh doesn't do is *understand* what it moves. Continuum is that middle layer: **it turns live events into always-current, 360-degree views of the things that matter** — this passenger, this flight, this bank of connections — **updated in milliseconds with every new event.** Pre-digested: the answer is computed once, when the event arrives — not re-derived a thousand times when a thousand consumers ask.

And it delivers those views three ways at once: **as events** pushed back onto the mesh — nothing polls; **as MCP endpoints** — an agent asks one question, gets one current, trusted object instead of ten tool calls it has to stitch together; **and as SQL** — for the NOC dashboards and analysts, same view, same numbers.

**One pre-digested source of truth — the same current answer for AI and humans alike.** The agent and the NOC never argue about whose number is right.

→ "One level down — quickly — because the *how* is what makes this trustworthy."

---

## SLIDE 3 — "Inside Continuum" (2:15–3:15)

[Point once at each element; do not linger:]

Streams come in **filtered by your topic hierarchy** — `flights/status/*`, `passengers/itinerary/*`, weather, crew, gates. Continuum subscribes like any well-behaved consumer on your mesh. No side-channel pipeline, no new firehose.

Inside, three ideas stack: **materialized views** — a saved question that never goes stale, like "every passenger currently in the air," maintained continuously, not re-run on a schedule. **Views built on views** — JOINs across different streams, so a dozen raw feeds become one business object, and one event ripples correctly through the whole stack. **Triggers** — views watch for conditions, and when one fires, the result lands **back on the mesh as a new event** for anything downstream to act on.

Bottom line, two words: **milliseconds** — the full graph updates per event, fast enough to act inside the moment — and **perfect consistency** — every view agrees at every instant. The summary and the detail never tell the NOC two different stories.

[If asked who builds it: "An analyst who knows SQL — not a streaming engineering team you have to staff."]

→ "Let's make it yours."

---

## SLIDE 4 — "Airline Rebooking, in Action" (3:15–4:45)

[This is their slide. Tell it as their Thursday:]

It's 5:40 PM and a cell parks over O'Hare. Inbounds slip five, fifteen, forty minutes — and somewhere in that terminal are a thousand passengers whose connections just went from comfortable to tight to *impossible*. The order in which you find out — and act — is everything.

Three feeds you already publish: **live flight status, itineraries, seats and connections.** Continuum joins them into one live view with one job: **for every passenger, right now — do they still make it?** Re-evaluated the instant anything changes, not every fifteen minutes.

The moment a connection becomes impossible — *that moment* — an event fires and the agent on Solace Agent Mesh handles **that specific passenger**: rebook onto the 7:15; last flight out, so hotel plus the 6 AM; or rebook *and* notify, boarding pass in the app before their inbound finishes taxiing. And the NOC sees **every at-risk passenger, live, on one dashboard — fed by the same view the AI is acting on.** One truth, human and machine.

You already run ConnectionSaver — you hold doors when the math works. This is that same instinct, **per passenger, network-wide, in seconds** — and when your team saw this pattern in August, the read was that the NOC is the cleanest path to an enterprise pattern. We agree.

[Close — 4:45–5:00:]

**Thousands of passengers, each handled in seconds — not after they've missed the flight.** The next step is small: a half-day working session — pick one NOC decision that happens too late today, define the view against your real streams, live in days. That's the ask. Thank you.

---

## Q&A — rapid-fire openers (if time allows)

**Flink?** — "Different tools, different people. Keep what's running — Continuum connects. **Flink is a framework you staff; Continuum is a capability you turn on.** The author is an analyst with SQL, not a streaming team you have to hire."

**We use MCP already.** — "Keep it — for actions and long-tail lookups. Ten MCP calls per question is slow, token-hungry, and never point-in-time consistent — each call lands at a different instant. Continuum pushes one pre-assembled, consistent object with the event that wakes the agent. **MCP for actions; Continuum for hot context.**"

**Real numbers?** — "Measured end to end — event in, through a three-view join cascade, back out to a consumer — **well under 100 milliseconds**, tens of thousands of events per second, zero duplicates across restarts. And cost scales with the rate of change, not the size of your history — it never re-scans."

---

*Compression rule if you're cut to 3 minutes on the spot: drop Slide 3 entirely, keep Slides 1, 2, and 4 — never drop the O'Hare story or the close.*
