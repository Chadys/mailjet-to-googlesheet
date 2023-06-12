from package.mailjet import (
    get_mailjet_data,
    get_mailjet,
    get_all_campaigns,
)
from package.sheets import (
    get_sheet_service,
    get_sheet_state,
    write_data_to_sheet,
    update_sheet_state,
)


def main():
    service = get_sheet_service()
    mailjet = get_mailjet()
    sheet_state = get_sheet_state(service)
    campaigns = get_all_campaigns(mailjet, sheet_state)
    update_sheet_state(service, sheet_state, campaigns)
    data = get_mailjet_data(mailjet, sheet_state, campaigns)
    write_data_to_sheet(service, data)


if __name__ == "__main__":
    main()
