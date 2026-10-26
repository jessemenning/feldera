# Presenter Script — `continuum-ai-value.pptx`

**Deck:** 4 slides — *Your AI Is Only as Good as What It Knows Right Now* → *What Continuum Actually Does* → *Inside Continuum* → *Airline Rebooking, in Action*
**Audience:** existing Solace customers; know the mesh, some AI awareness, no stream-processing background
**Run time:** ~12 minutes spoken + Q&A (per-slide timings below)
**Voice:** friendly enterprise architect — direct, concrete, no jargon, no marketing fluff
**Hard rules:** the product is **Continuum** — never name the underlying engine. Say **"Solace Platform"**, never "PubSub+". Never cut a money line to save time — cut a sentence of tech mechanics instead.

*Stage directions are in [brackets]. Words in **bold** are the beats to hit even if you compress everything around them. A "→ TRANSITION" line ends each slide — say it while advancing.*

---

## SLIDE 1 — "Your AI Is Only as Good as What It Knows Right Now" (~3 min)

[Stand still for this one. No pointing, no pacing — this slide is about letting a problem land. Open before you touch the content:]

Thanks for the time today. I want to spend about fifteen minutes on something we've added to Solace Platform, and I want to start not with the product, but with a question you're probably already living with.

[Beat. Then, gesturing loosely at the intro block:]

You're putting AI in charge of real decisions. Maybe not everywhere yet — but somewhere. Spotting fraud. Approving payments. Helping customers. Running operations. And every one of you has a roadmap where that list gets longer next year, not shorter.

Here's the uncomfortable part. **Most AI today acts on data that's minutes or hours old** — and scattered across a dozen systems. The model might be brilliant. What it *knows* is stale. So it does one of three things: it guesses, it acts on history, or it slows down and wastes tokens asking around — call after call, system after system, trying to assemble a picture of *right now* from parts that were each true at a *different* moment.

And you feel it in the results. [Point to the four ✗ marks, one at a time, slowly:]

**Wrong calls on old facts.** The AI decides based on what was true five minutes ago. The account was fine five minutes ago. The connection was makeable five minutes ago. It isn't now — and the AI has no idea. And here's what makes this dangerous rather than just annoying: **stale context is the number one reason an agent acts wrong and sounds certain doing it.** It doesn't hedge. It doesn't say "my data might be old." It confidently does the wrong thing.

**Too slow in the moment.** Even when the AI eventually assembles the right answer, it arrives after it mattered. The fraud already cleared. The passenger already missed the connection. In these use cases, a correct answer that's late is just a well-documented failure.

**No trust.** This is the quiet one. Your people can't tell whether what the AI knows is current or complete — so they double-check everything, and now you've got an expensive assistant nobody quite believes. Trust is the actual adoption blocker for agentic AI, and freshness is where trust dies first.

**Rising cost.** Every single request re-fetches and re-computes the same answer. A thousand agents asking "what's the state of this account" a thousand times an hour — each one paying in tokens, in latency, and in load on your systems of record — to re-derive an answer that barely changed.

[Pause. Then the close, slower:]

Here's the thing I'd ask you to hold onto for the next ten minutes: **the winners in your industry will be the ones whose AI acts on what's true this second.** Not this hour. Not this batch window. This second.

→ TRANSITION: "And the reason I'm having this conversation with *you* — and not with the general market — is that you already own the hardest part of the solution. Let me show you."

---

## SLIDE 2 — "What Continuum Actually Does" (~3 min)

[This is the money slide. Walk the diagram left to right, physically. Start at the left box:]

You already run a Solace event mesh. Which means the moment-by-moment truth of your business — every order, every payment, every status change — is *already flowing*, live, in milliseconds. **Every event, live.** That's the left side of this picture, and you built it years ago.

What's been missing is the middle. The mesh *moves* events beautifully. It doesn't *understand* them. Nobody's been keeping a running answer up to date as those events fly past.

[Move to the center box:]

That's Continuum. **It turns your live events into always-current, 360-degree views of the things that matter most.** The customer. The passenger. The payment. The shipment. You define what that view looks like — once — and Continuum keeps it true, **updated in milliseconds, with every new event.**

I want to stress the word "pre-digested." Continuum doesn't wait for a question and then scramble to compute an answer. It combines, transforms, and time-windows your event streams *continuously*, so the answer already exists the instant anyone — human or AI — wants it. The work happens once, when the event arrives. Not a thousand times, when a thousand consumers ask.

[Move to the three output boxes:]

And then — because an answer nobody can reach is worthless — it delivers those views **three ways, instantly:**

**As events.** The moment a view changes in a way that matters, that change lands right back on your mesh as a new event. Anything that can subscribe — an agent, a microservice, an alerting system — gets *told*, immediately. Nothing polls.

**As MCP endpoints.** If you're building with AI agents, this is the plug-in point. Your agents ask one question and get one pre-assembled, current, trusted object — instead of making ten tool calls and stitching the answers together themselves.

**As SQL queries.** For your people. Dashboards, analysts, ops teams — they query the same live views directly, with the skill they already have.

[Step back from the screen. Land the takeaway:]

That's the sentence to remember: **one pre-digested source of truth — the same current answer, as events, MCP, or SQL, for AI and humans alike.** Your agent and your ops dashboard are never arguing about which of them has the right number, because there's only one number.

→ TRANSITION: "Now — 'always-current 360-degree view' is easy to say. Let me go one level down and show you what's actually happening inside, because the *how* matters here."

---

## SLIDE 3 — "Inside Continuum" (~3 min)

[One level more technical — this is the slide for the architect in the room. Keep energy conversational; you're explaining, not selling. Start at the input boxes on the left:]

Many streams come in. And notice they arrive **already filtered by your topic hierarchy** — this is your existing Solace discipline paying off. Flight status on `flights/status/*`, itineraries on `passengers/itinerary/*`, weather, crew, gates. Continuum subscribes to precisely the slices it needs, the same way any well-behaved consumer on your mesh does. No new firehose, no side-channel pipeline into some separate data platform. It's a native citizen of the mesh.

[Move to the three stacked boxes in the center, in order:]

Inside, three simple ideas stack on top of each other.

First: **materialized views.** Think of a view as a saved question that never goes stale. "The last sixty seconds of transactions, by region." "Every passenger currently in the air." You isolate the fields that matter, filter by value, filter by *time*. And unlike a report, it's not re-run on a schedule — it's *maintained*, continuously, as each event arrives.

Second — and this is where it gets genuinely powerful — **views build on views.** You JOIN across completely different streams. Flight status joined to itineraries joined to seat inventory. Each layer stays current automatically, because when one event touches the bottom of the stack, the change ripples up through every view built on top of it. That's how a dozen raw feeds become *one* business object.

Third: **triggers.** Views watch for specific conditions — a threshold crossed, a status flipped, a join that suddenly produces a row it shouldn't. And when a trigger fires, the result goes [point to the output box] **right back onto the event mesh as a new event.** Which means the *output* of Continuum is a first-class input to everything else you run. Events in, intelligence out — same fabric, both directions.

[Pause on the takeaway bar:]

Two words on this bottom line, because they're doing a lot of work. **Milliseconds:** the whole graph — ingest, views, joins, triggers, back out — updates in milliseconds per event. Not "fast for a batch job." Fast enough to act *within* the moment. And **perfect consistency:** every view in that stack agrees at every instant. You will never catch the summary view and the detail view telling you two different stories, because one computation keeps them true together. If you've ever had a dashboard argue with the report underneath it, you know exactly why I'm calling this out.

[If — and only if — asked "who writes all this?": "An analyst who knows SQL. That's the whole staffing story. This is not a framework you hire a streaming engineering team to operate — it's a capability of the platform you already run."]

→ TRANSITION: "So that's the machinery. Machinery is only interesting if it changes an outcome — so let me make this concrete with a scenario every one of us has personally suffered through."

---

## SLIDE 4 — "Airline Rebooking, in Action" (~2.5 min)

[Tell this as a story, not a walkthrough. Slow down. Start before pointing at anything:]

It's 5:40 on a Thursday evening. A weather cell parks itself over the hub. Inbound flights start slipping — five minutes, fifteen, forty. Somewhere in that airport are eleven hundred passengers whose connections just went from comfortable, to tight, to *impossible* — and the order in which the airline finds out matters enormously.

[Now the left inputs:]

Three streams the airline already has on the mesh: **live flight status. Passenger itineraries. Seats and connections.**

[Center box:]

Continuum joins them into one live view with one job: **for every passenger in the system, right now — can they still make their connection?** Not a report run every fifteen minutes. A living answer, per passenger, re-evaluated the instant any status changes.

[Right side, first output:]

The moment flight 1182's new arrival time makes a connection impossible — not when a batch job notices, not when the passenger walks up to a gate agent, but *that moment* — an event fires, and the AI on Solace Agent Mesh decides how to handle **that specific passenger**. Rebook onto the 7:15. Or it's the last flight out — book the hotel and the morning departure. Or rebook *and* notify, with the new boarding pass already in the app before the plane they're sitting on has finished taxiing.

[Second output:]

And ops sees the whole picture live — **every at-risk passenger, ranked, on one dashboard** — fed by the *same view* the AI is acting on. When a passenger calls, the human agent and the AI agent are looking at the same truth. Remember the trust problem from slide one? This is what solving it looks like.

[Takeaway, then close it out:]

**Thousands of passengers, each handled in seconds — not after they've already missed the flight.** That's the difference between an airline that recovers a bad evening and one that trends on social media.

[Final beat — bring it home to *their* business:]

And here's why I told you an airline story even if you don't run one: the pattern is completely general. Fraud that's blocked mid-transaction instead of clawed back. Orders rescued before the SLA breaches. Positions hedged inside the window instead of after it. **Anywhere your business has a moment where acting *now* beats explaining *later* — that's a Continuum use case.** You already have the events. You already have the mesh. Continuum is the piece that turns them into decisions.

The next step, if this resonated, is small: a half-day workshop. Pick one "act on it right now" decision, define that one view, and see it running against your real streams in days. Talk to your account team — or to me, right after this.

Thank you.

---

## Q&A — likely questions, word-for-word openers

**"How is this different from Flink?"**
"Genuinely different tools for different people. Flink is a powerful framework — and if you have Flink pipelines running today that you're happy with, keep them; Continuum connects to the same mesh. The difference is staffing: **Flink is a framework you staff — Continuum is a capability you turn on.** The person who builds a Continuum view is an analyst who knows SQL, not a streaming engineer you're competing with three other companies to hire. This is for the eighty percent of cases that are joins and always-current views — and for every team that will never staff a streaming platform group."

**"We already use MCP to ground our agents. Doesn't this overlap?"**
"Keep MCP — it's exactly right for *actions* and long-tail lookups. The problem is the hot path: the context your agents need constantly, in the same shape every time. Fanning out ten MCP calls per question is slow, token-expensive, hammers your systems of record — and because each call lands at a different instant, the stitched-together picture was never true at any single moment. Continuum is the complement: one pre-assembled, point-in-time-consistent object, pushed to the agent with the very event that woke it. **MCP for actions; Continuum for hot context.**"

**"What does 'milliseconds' actually mean? Do you have numbers?"**
"Measured end to end — event published, through the broker, through a three-view cascade of joins, back onto the mesh, into the consumer — we see **well under 100 milliseconds**, with tens of thousands of events per second sustained, and zero duplicates or losses verified across restarts. The honest caveat: your mileage depends on your views. But the architecture never re-scans your history — cost and latency scale with the *rate of change*, not the size of your data."

**"Isn't this just a fast cache or a fancy dashboard?"**
"A cache holds an answer that somebody computed once — and goes stale the moment the world moves. Continuum *maintains* the answer: every arriving event updates it incrementally, and every view in the graph stays consistent with every other. It even corrects itself with no new input — when a transaction ages out of a sixty-second window, the number comes down on its own. A cache can't do that; a dashboard on a ten-minute-old database definitely can't."

**"What does it cost / what's the commercial model?"**
"It's a Solace Platform capability, sold alongside your existing platform agreement — your account team will scope it to your use case. The economic argument I'd focus on is the one you can measure: you're almost certainly paying for this capability today, either as a streaming engineering team, a warehouse being polled too hard, or agents re-deriving the same answer in tokens all day long. Continuum consolidates that spend onto the platform you already run." *(Never skip this answer to save time.)*

**"Who operates it? What's the ops burden?"**
"There's no cluster to size, no state stores to tune, no framework to patch. It runs as part of Solace Platform, and the day-two experience is intentionally boring: define views in SQL, they stay true. The team you already have is the team it needs."

**"How do we start?"**
"Half-day workshop. Bring one decision your business makes too late today, and the two or three event streams that inform it. We define the business object together in SQL, and the goal is a live view running against your real streams — feeding a dashboard and an agent — in days, not quarters."

---

*Timing guide: Slide 1 ≈ 3:00 · Slide 2 ≈ 3:00 · Slide 3 ≈ 3:00 · Slide 4 ≈ 2:30 · close + buffer ≈ 0:30 — ~12 min total, leaving 3+ min for Q&A in a 15-minute slot. If you must compress: cut from slide 3 (mechanics), never from slide 1 (stakes) or the slide 4 close.*
