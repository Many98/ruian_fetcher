import argparse
import os
import logging
from ruian import RuianFetcher


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="-------------------------------RUIAN API Fetcher ------------------------------- \n"
                    "----------------------- Wrapper over RUIAN API services ---------------\n"

                    "Capabilities: \n"
                    "1) Obtain `kod adresniho mista` given address (which is default)\n"
                    "2) Obtain coordinates given address \n"
                    "3) Process multiple addresses (either code or coordinates)\n"

        ,

        formatter_class=argparse.RawTextHelpFormatter,
    )

    parser.add_argument(
        "address",
        metavar='N', type=str, nargs='*',  # argument can take zero or more values
        help="Addresses to be parsed. Can be zero, one or multiple addressses. E.g. `address_A address_B address_C`"

    )

    parser.add_argument(
        "--coordinates",
        "-c",
        action='store_true',
        help="Type of task. By default `kod adresniho mista` is fetched. if specified this flag i.e. `--coordinates` or `-c` then coordinates will be fetched instead"

    )

    parser.add_argument(
        "--column_name",
        "-cn",
        type=str,
        help="Name of column where are stored addresses.",
        default="unspecified"

    )

    parser.add_argument(
        "--in_file",
        "-if",
        type=str,
        help="Path to input excel/csv file. Should contain extension as it is used for file type derivation.",
        default=""
    )

    parser.add_argument(
        "--server",
        "-s",
        type=str,
        help="Name of server",
        default=""

    )

    parser.add_argument(
        "--database",
        "-d",
        type=str,
        help="Name of database",
        default=""

    )

    parser.add_argument(
        "--in_table",
        "-it",
        type=str,
        help="Name of input table.",
        default=""

    )

    parser.add_argument(
        "--out_file",
        "-of",
        type=str,
        help="Path to output excel/csv file. Should contain extension as it is used for file type derivation.",
        default=""

    )

    parser.add_argument(
        "--out_table",
        "-ot",
        type=str,
        help="Name of output table.",
        default=""

    )

    args = parser.parse_args()

    r = RuianFetcher()

    addresses_to_be_processed = tuple(args.address)
    data_status = True

    if not addresses_to_be_processed:
        if args.in_file or (args.server and args.in_table and args.db):
            logging.info("No addresses provided as argument. Proceeding with data loading...")
            if args.column_name == "undefined":
                data_status = False
                logging.info("it seems that no `column_name` was provided. Please provide column name if you want load data from file/db using `-cn <column name>`")
        else:
            data_status = False
            logging.error("No addresses nor input files provided as argument. Aborting...")
            

    else:
        logging.info("Addreses provided as argument. Proceeding with these values...")

    if args.out_file:
        pass
    elif (not args.server or not args.db or not args.out_table) and data_status: 
        args.out_file = f"address_{'coor' if args.coordinates else 'code'}_processed.csv"
        logging.info(f"No valid export method specified. Data will be exported to {args.out_file} file in current working directory")
        logging.info(f"Current working directory is {os.getcwd()}")
        

    if args.coordinates and data_status:
        logging.info("Quering Coordinates API")
        try:
            r.bulk_fetch_coordinates(addresses_to_be_processed, args.in_file, args.server, args.database, args.in_table, args.column_name, args.out_file, args.out_table, export=True)
        except Exception as e:
            logging.error(str(e))
            raise

        logging.info("Data processed and exported successfuly.")
    elif data_status:
        logging.info("Quering RUIAN Code API")

        try:
            r.bulk_fetch_ruian_codes(addresses_to_be_processed, args.in_file, args.server, args.database, args.in_table, args.column_name, args.out_file, args.out_table, export=True)
        except Exception as e:
            logging.error(str(e))
            raise
        
        logging.info("Data processed and exported successfuly.")
    else:
        pass
