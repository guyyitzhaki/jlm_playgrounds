# jlm_playgrounds
Latest version deployed on https://play-jlm.github.io/jlm_playgrounds/

# Heroku
This repository is associated with our heroku account, auto deploy is enabled. To deploy the express webapp (entrypoint: index.js) just push to branch `deploy`.
Files belong to heroku project: `.env`, `Procfile`, `index.js`, `package.json`, `lookup/`

## Usage
1. `git clone`
2. `npm i`
3. Setup environment variables for credentials (should be the same as Heroku config). See drive documentation for more infromation.
4. Using heroku CLI, run `heroku local`
5. Hit `http://localhost:5000/trigger`

# Docs directory
This directory includes static files for the map. This directory is being served by GitHub pages every time you push to `master` branch. Please note that it takes a few minutes for new resources to get served in GH pages.
Note that some resources are being used by the Google script form.

## Usage
Just serve the `/docs` directory and enter `index.html`.