# 🏀 Power 4 NBA Fantasy — Daily Projections Website

A fully automated NBA fantasy website that:
- Scrapes lineups.com projections **every day at noon ET**
- Analyzes the best Power 4 lineups by DVP matchup
- Displays everything on a slick, mobile-friendly website
- Has ad slots built in for Google AdSense monetization

---

## 🚀 HOW TO DEPLOY (Step-by-Step, No Coding Needed)

### STEP 1 — Create a GitHub Account (if you don't have one)
1. Go to **github.com** and sign up for free
2. Verify your email

### STEP 2 — Upload This Project to GitHub
1. On github.com, click the **"+"** button (top right) → **"New repository"**
2. Name it `power4-nba` — keep it **Public**
3. Click **"Create repository"**
4. Click **"uploading an existing file"**
5. Drag and drop ALL the files from this folder into the window
6. Click **"Commit changes"**

### STEP 3 — Create a Railway Account
1. Go to **railway.app** and sign up with your GitHub account
2. Click **"New Project"**
3. Click **"Deploy from GitHub repo"**
4. Select your `power4-nba` repository
5. Railway will automatically detect it's a Python app and deploy it

### STEP 4 — Get Your Live URL
1. In Railway, click your project → click **"Settings"**
2. Under **"Domains"**, click **"Generate Domain"**
3. You'll get a free URL like: `power4-nba.up.railway.app`
4. Share that link — anyone can visit it!

### STEP 5 — Trigger First Scrape
1. Visit `https://YOUR-URL.up.railway.app/refresh`
2. This runs the scraper immediately and populates the site
3. After that, it auto-runs every day at noon ET

---

## 💰 HOW TO ADD GOOGLE ADSENSE

1. Go to **google.com/adsense** and sign up with your Google account
2. Add your website URL when prompted
3. Wait for approval (usually 1-3 days)
4. Once approved, go to **Ads → By ad unit → Display ads**
5. Copy the `<script>` code Google gives you
6. Open `templates/index.html` in any text editor
7. Find the two sections that say `ADVERTISEMENT`
8. Replace them with your AdSense code
9. Push the update to GitHub — Railway auto-deploys it

---

## 📁 FILE STRUCTURE

```
power4-nba/
├── app.py              ← Main web server + scheduler
├── scraper.py          ← Lineups.com scraper + analysis engine
├── requirements.txt    ← Python packages needed
├── Procfile            ← Tells Railway how to start the app
├── railway.toml        ← Railway config
├── templates/
│   └── index.html      ← The website design
└── data/               ← Auto-created, stores daily reports
    └── latest_report.json
```

---

## 🔧 URLS

| URL | What it does |
|-----|-------------|
| `/` | Main dashboard |
| `/refresh` | Manually trigger a scrape right now |
| `/api/report` | Raw JSON data (for developers) |

---

## ❓ TROUBLESHOOTING

**Site says "No Data Yet"**
→ Visit `/refresh` to manually run the scrape

**Scrape fails / no projections**
→ lineups.com may have changed their page layout. Open an issue or check the logs in Railway under "Deployments → View Logs"

**How do I update the site?**
→ Edit files in GitHub directly (click any file → pencil icon). Railway auto-deploys on every save.

---

## 💡 TIPS FOR GROWING YOUR AUDIENCE

- Share the link on **Twitter/X** with #DFS #NBAFantasy #PowerPlay tags
- Post daily screenshots to Instagram/TikTok
- Add a Discord server invite to the site header
- Post in NBA DFS subreddits (r/dfsports, r/dfs)

---

Built with Python, Flask, and a lot of basketball knowledge 🏀
