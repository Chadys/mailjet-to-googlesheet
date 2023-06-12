import datetime

from mailjet_rest import Client

from .models import SheetState, Campaign
from .utils import divide_or_zero, env, now, retry_with_backoff

MJ_APIKEY_PUBLIC = env.str("MJ_APIKEY_PUBLIC")
MJ_APIKEY_PRIVATE = env.str("MJ_APIKEY_PRIVATE")

headers = {
    "campaign": [
        "date",
        "nom de la campagne",
        "nombre d'envoi",
        "nombre de mail bloqués",
        "nombre erreur temporaire",
        "nombre erreur permanente",
        "nombre mails ouverts",
        "nombre total d'ouverture",
        "nombre de clics des msg délivrés",
        "nombre de mail cliqués",
        "nombre total de click",
        "nombre de désabonnements",
        "nombre de spam",
        "délai d'ouverture moyen (en sec)",
        "délai de clics moyens (en sec)",
    ],
    "link": [
        "nom de la campagne",
        "URL",
        "nombre de mail cliqués",
        "nombre total de click",
        "position de l'URL link dans le contenu HTML",
    ],
    "user_agent": [
        "nom de la campagne",
        "plateforme",
        "user agent description",
        "nombre de mail cliqués",
        "nombre total de click",
    ],
    "region": [
        "nom de la campagne",
        "pays",
        "nombre de mail cliqués",
        "nombre de mail ouvert",
    ],
}


def get_mailjet() -> Client:
    return Client(auth=(MJ_APIKEY_PUBLIC, MJ_APIKEY_PRIVATE))


def call_mailjet_api(mailjet_fn: Client, filters: dict):
    result = mailjet_fn.get(filters=filters)
    assert result.status_code == 200, result.status_code
    return result.json()["Data"]


def get_all_campaigns(mailjet: Client, sheet_state: SheetState) -> list[Campaign]:
    filters = {
        "Period": sheet_state.campaign_period,
        "Limit": 1000,
        "Sort": "SendStartAt",
    }
    data = retry_with_backoff(lambda: call_mailjet_api(mailjet.campaign, filters))
    return [
        Campaign(
            campaign_data["ID"],
            f'{campaign_data["FromName"]} - {campaign_data["CreatedAt"]} - {campaign_data["Subject"]}',
        )
        for campaign_data in data
    ]


def get_mailjet_campaign_data(
    mailjet: Client,
    campaign: Campaign,
    time_range: tuple[str, str],
    sheet_state: SheetState,
) -> dict:
    data_type = "campaign"
    filters = {
        "CounterSource": "Campaign",
        "CounterTiming": "Event",
        "CounterResolution": "Day",
        "FromTS": time_range[0],
        "ToTS": time_range[1],
        "SourceID": campaign.id,
    }
    data = retry_with_backoff(lambda: call_mailjet_api(mailjet.statcounters, filters))

    values = [
        [
            day_data["Timeslice"][:10],  # "date", remove time part
            campaign.title,  # "nom de la campagne",
            day_data["MessageSentCount"],  # "nombre d'envoi",
            day_data["MessageBlockedCount"],  # "nombre de mail bloqués",
            day_data["MessageSoftBouncedCount"],  # "nombre erreur temporaire",
            day_data["MessageHardBouncedCount"],  # "nombre erreur permanente",
            day_data["MessageOpenedCount"],  # "nombre mails ouverts",
            day_data["EventOpenedCount"],  # "nombre total d'ouverture",
            day_data["EventClickedCount"],  # "nombre de clics des msg délivrés"
            day_data["MessageClickedCount"],  # "nombre de mail cliqués",
            day_data["EventClickedCount"],  # "nombre total de click",
            day_data["MessageUnsubscribedCount"],  # "nombre de désabonnements",
            day_data["MessageSpamCount"],  # "nombre de spam",
            divide_or_zero(
                day_data["EventOpenDelay"], day_data["MessageOpenedCount"]
            ),  # "délai d'ouverture moyen (en sec)",
            divide_or_zero(
                day_data["EventClickDelay"], day_data["MessageClickedCount"]
            ),  # "délai de clics moyens (en sec)",
        ]
        for day_data in data
    ]
    if not sheet_state.has_headers:
        values = [headers[data_type], *values]
        sheet_state.new_rows[data_type] = 1
    data = {
        "range": f"Darty-{data_type}!A{sheet_state.new_rows[data_type]}",
        "values": values,
    }
    sheet_state.new_rows[data_type] += len(values)
    return data


def get_mailjet_link_data(
    mailjet: Client,
    campaign: Campaign,
    sheet_state: SheetState,
) -> list:
    data_type = "link"
    filters = {
        "CampaignID": campaign.id,
        "Sort": "ClickedEventsCount+DESC",
        "Limit": 10,
    }
    data = retry_with_backoff(
        lambda: call_mailjet_api(getattr(mailjet, "statistics_link-click"), filters)
    )
    row_data = []
    new_data = []
    for link_data in data:
        value = [
            campaign.title,  # "nom de la campagne",
            link_data["URL"],  # "URL",
            link_data["ClickedMessagesCount"],  # "nombre de mail cliqués",
            link_data["ClickedEventsCount"],  # "nombre total de click",
            link_data[
                "PositionIndex"
            ],  # "position de l'URL link dans le contenu HTML",
        ]
        row = sheet_state.link_mapping.get((campaign.title, link_data["URL"]))
        if row is not None:
            row_data.append(
                {
                    "range": f"Darty-{data_type}!A{row}",
                    "values": [value],
                }
            )
        else:
            new_data.append(value)

    if new_data:
        row_data.append(
            {
                "range": f"Darty-{data_type}!A{sheet_state.new_rows[data_type]}",
                "values": new_data,
            }
        )
        sheet_state.new_rows[data_type] += len(new_data)
    if not sheet_state.has_headers:
        row_data = [
            {
                "range": f"Darty-{data_type}!A1",
                "values": [headers[data_type]],
            },
            *row_data,
        ]
    return row_data


def get_mailjet_user_agent_data(
    mailjet: Client, campaign: Campaign, sheet_state: SheetState
) -> list:
    data_type = "user_agent"
    filters = {
        "CampaignID": campaign.id,
        "Event": "open",
        "Limit": 1000,
        "Sort": "Count+DESC",
    }
    data = retry_with_backoff(
        lambda: call_mailjet_api(mailjet.useragentstatistics, filters)
    )
    new_data = []
    row_data = []
    for user_agent_data in data:
        value = [
            campaign.title,  # "nom de la campagne",
            user_agent_data["Platform"],  # "plateforme",
            user_agent_data["UserAgent"],  # "user agent description",
            user_agent_data["DistinctCount"],  # "nombre de mail cliqués",
            user_agent_data["Count"],  # "nombre total de click",
        ]
        row = sheet_state.user_agent_mapping.get(
            (campaign.title, user_agent_data["Platform"], user_agent_data["UserAgent"])
        )
        if row is not None:
            row_data.append(
                {
                    "range": f"Darty-{data_type}!A{row}",
                    "values": [value],
                }
            )
        else:
            new_data.append(value)

    if new_data:
        row_data.append(
            {
                "range": f"Darty-{data_type}!A{sheet_state.new_rows[data_type]}",
                "values": new_data,
            }
        )
        sheet_state.new_rows[data_type] += len(new_data)

    if not sheet_state.has_headers:
        row_data = [
            {
                "range": f"Darty-{data_type}!A1",
                "values": [headers[data_type]],
            },
            *row_data,
        ]
    return row_data


def get_mailjet_region_data(
    mailjet: Client, campaign: Campaign, sheet_state: SheetState
) -> list:
    data_type = "region"
    filters = {
        "CampaignID": campaign.id,
        "Limit": 1000,
        "Sort": "OpenedCount+DESC",
    }
    data = retry_with_backoff(lambda: call_mailjet_api(mailjet.geostatistics, filters))
    new_data = []
    row_data = []
    for region_data in data:
        value = [
            campaign.title,  # "nom de la campagne",
            region_data["Country"],  # "pays",
            region_data["ClickedCount"],  # "nombre de mail cliqués",
            region_data["OpenedCount"],  # "nombre de mail ouvert",
        ]
        row = sheet_state.region_mapping.get((campaign.title, region_data["Country"]))
        if row is not None:
            row_data.append(
                {
                    "range": f"Darty-{data_type}!A{row}",
                    "values": [value],
                }
            )
        else:
            new_data.append(value)

    if new_data:
        row_data.append(
            {
                "range": f"Darty-{data_type}!A{sheet_state.new_rows[data_type]}",
                "values": new_data,
            }
        )
        sheet_state.new_rows[data_type] += len(new_data)

    if not sheet_state.has_headers:
        row_data = [
            {
                "range": f"Darty-{data_type}!A1",
                "values": [headers[data_type]],
            },
            *row_data,
        ]
    return row_data


def get_time_range(sheet_state: SheetState) -> tuple[str, str]:
    last_day = now - datetime.timedelta(days=1)
    first_day = last_day - datetime.timedelta(
        days=100
    )  # mailjet only send data for 100 days maximum
    if sheet_state.last_campaign_date is not None:
        next_day_campaign = sheet_state.last_campaign_date + datetime.timedelta(days=1)
        first_day = max(
            first_day,
            datetime.datetime.combine(next_day_campaign, datetime.time()),
        )
    return first_day.isoformat(), last_day.isoformat()


def get_mailjet_data(
    mailjet: Client, sheet_state: SheetState, campaigns: list[Campaign]
) -> list:
    time_range = get_time_range(sheet_state)
    data = []
    if time_range[0] >= time_range[1]:
        return data
    for campaign in campaigns:
        data.extend(
            [
                get_mailjet_campaign_data(mailjet, campaign, time_range, sheet_state),
                *get_mailjet_link_data(mailjet, campaign, sheet_state),
                *get_mailjet_user_agent_data(mailjet, campaign, sheet_state),
                *get_mailjet_region_data(mailjet, campaign, sheet_state),
            ]
        )
        sheet_state.has_headers = True
    return data
