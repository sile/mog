use slack_hook2::{PayloadBuilder, Slack};

#[derive(Debug, structopt::StructOpt)]
#[structopt(rename_all = "kebab-case")]
pub struct SlackWebhookOpt {
    #[structopt(long)]
    pub slack_url: Option<url::Url>,
}

impl SlackWebhookOpt {
    pub fn build(&self) -> anyhow::Result<SlackWebhookClient> {
        let slack = if let Some(url) = self.slack_url.clone() {
            Some(Slack::new(url)?)
        } else {
            None
        };
        Ok(SlackWebhookClient { slack })
    }
}

#[derive(Debug)]
pub struct SlackWebhookClient {
    slack: Option<Slack>,
}

impl SlackWebhookClient {
    pub async fn post(&self, message: &str) -> anyhow::Result<()> {
        if let Some(slack) = &self.slack {
            let payload = PayloadBuilder::new().text(message).build()?;
            let _ = slack.send(&payload).await; // TODO: warning message
        }
        Ok(())
    }
}
