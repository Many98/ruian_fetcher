## Wrapper over RUIAN API

Services we use:
- [Ruian Codes API](https://vdp.cuzk.cz/vdp/ruian)
- [Coordinates API](https://ags.cuzk.cz/arcgis/rest/services/RUIAN/Vyhledavaci_sluzba_nad_daty_RUIAN/MapServer/exts/GeocodeSOE/findAddressCandidates)

#### Notes:
- Currently is not implemented any limiting strategy so use it with caution to now overload server
- We use existing API which implements own search strategy therefore valid response is not guaranteed
- In some cases for one address there may be multiple match candidates. All candidates are exported.

### Capabilities

- Obtain code given address (so called Kod adresniho mista)
- Obtain coordinates given address
- Process multiple addresses (either code or coordinates)

### Installation

1. Project was written in Python 3.12 therefore try to ensure you use it
2. Clone project: ```git clone https://github.com/Many98/ruian_fetcher.git && cd ruian_fetcher```
3. Create environment: `python -m venv myenv`
4. Only on Windows machine temporarily  change execution policy for session by command `Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope Process`
5. Activate environment: `.\myenv\Scripts\Activate.ps1`
6. Install requirements: `pip install -r requirements.txt`


### CLI Usage
```
-------------------------------RUIAN API Fetcher -------------------------------

----------------------- Wrapper over RUIAN API services ---------------

Capabilities:
1) Obtain `kod adresniho mista` given address (which is default)
2) Obtain coordinates given address
3) Process multiple addresses (either code or coordinates)

positional arguments:
  N                     Addresses to be parsed. Can be zero, one or multiple addressses. E.g. `address_A address_B address_C`

options:
  -h, --help            show this help message and exit
  --coordinates, -c     Type of task. By default `kod adresniho mista` is fetched. if specified this flag i.e. `--coordinates` or `-c` then coordinates will be fetched instead
  --column_name COLUMN_NAME, -cn COLUMN_NAME
                        Name of column where are stored addresses.
  --in_file IN_FILE, -if IN_FILE
                        Path to input excel/csv file. Should contain extension as it is used for file type derivation.
  --server SERVER, -s SERVER
                        Name of server
  --database DATABASE, -d DATABASE
                        Name of database
  --in_table IN_TABLE, -it IN_TABLE
                        Name of input table.
  --out_file OUT_FILE, -of OUT_FILE
                        Path to output excel/csv file. Should contain extension as it is used for file type derivation.
  --out_table OUT_TABLE, -ot OUT_TABLE
                        Name of output table.
```
#### CLI Examples

- To obtain help use: ```python main.py --help```
- To fetch ruian codes for 2 addresses: ```python main.py "Letovice, Rekreační č.p. 191, PSČ 67961, Česká republika" "Třída Tomáše Bati 941, Otrokovice, 76502, Česká republika"```
- To fetch coordinates for 2 addresses: ```python main.py "Letovice, Rekreační č.p. 191, PSČ 67961, Česká republika" "Třída Tomáše Bati 941, Otrokovice, 76502, Česká republika" --coordinates```

### API Usage
- #### TODO: Create API using fastAPI
- #### TODO: Async version of methods
- #### TODO: Implement/Use some limiter
