# mailjet-to-googlesheet

This app take some data from Mailjet and insert it in a GoogleSheet (to be used by Data Studio)

### How to run
First you'll need to create a "secrets.env" file, by filling the placeholder values from secrets_sample.env.
You'll also need to download the Google client secret file generated for this app, rename it "credentials.json"
and put it in [package](package) folder.

Use `docker-compose run --rm main` to run (because we need to interact with console)

**This project is deprecated and not compatible anymore with new google sheet API.
It might still work to fetch data from mailjet but with no warranty as this is not maintained**
