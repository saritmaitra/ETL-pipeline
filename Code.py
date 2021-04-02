import datetime, pickle, copy
import pandas as pd
import csv, json, time, sys


start = time.perf_counter()
with open("dataset.csv") as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=",")
    line_count = 0
    for row in csv_reader:
        if line_count == 0:
            print(f'Column names are {", ".join(row)}')
            line_count += 1
        else:
            # print(f'\t{row[0]} works in the {row[1]} department, and was born in {row[2]}.')
            line_count += 1

    print(
        f"Processed {line_count} lines."
    )  # name of columns and numbers of rows in data set


def csv_to_json(in_file_csv, out_file_json):
    json_array = []

    # read csv file
    with open(in_file_csv, "r", encoding="utf-8") as csv_file:
        # load csv file data using csv library's dictionary reader
        csv_reader = csv.DictReader(csv_file)

        # convert each csv row into python dict
        for row in csv_reader:
            # add the abobe python dict to json array
            json_array.append(row)

    # convert python jsonArray to JSON String and write to file
    with open(out_file_json, "w") as json_file:
        json_string = json.dumps(json_array, indent=4)
        json_file.write(json_string)


in_file_csv = r"dataset.csv"
out_file_json = r"dataset.json"


csv_to_json(in_file_csv, out_file_json)

# print(f"Conversion completed successfully in {finish - start:0.4f} seconds")

with open("dataset.json") as json_data:
    d = json.load(json_data)
    # print(type(d))
    # print(type(d[0]))
    # print(json.dumps(d[0], indent=2, sort_keys=False))


def unflatten(somedict):
    unflattened = {}

    for key, value in somedict.items():
        splitkey = key.split(".")
        # print(f"doing {key} {value} {splitkey}")

        # subdict is the dict that goes deeper in the nested structure
        subdict = unflattened

        for subkey in splitkey[:-1]:

            # if this is the first time we see this key, add it
            if subkey not in subdict:
                subdict[subkey] = {}

            # shift the subdict a level deeper
            subdict = subdict[subkey]

        # add the value
        subdict[splitkey[-1]] = value

    return unflattened


data = {
    "key": "8af5e2b9b71ad7c9e642eb2ae2d07b4f",
    "name": "8af5e2b9b71ad7c9e642eb2ae2d07b4f",
    "colour": "77e774e6cc4d94d6a32f6256f02d9552",
    "website": "d827833ee83ac52c792140a7ace0bf58",
    "logo": "289b3c43c655e143fccb5f5405b82915",
    "lastUpdate": "",
    "location.region": "2fecdba5c1f3efe428fe6dca4223cc8e",
    "location.country": "4442e4af0916f53a07fb8ca9a49b98ed",
    "location.city": "1fc91e86962825bb745de53d1657b3e4",
    "stats.founded": "a00e5eb0973d24649a4a920fc53d9564",
    "stats.employee.min": "2838023a778dfaecdc212708f721b788",
    "stats.employee.max": "3644a684f98ea8fe223c713b77189a77",
    "stats.patents": "",
    "stats.revenue": "",
    "stats.printersSold": "7647966b7343c29048673252e490f736",
    "business.valueChainPosition": "b9afc2aee4bc78199045de2a5f52cb4a",
    # "business.valueChainPosition.type": "843b40c19e1c23357453d8f8be178362",
    "business.strategy": "",
    "business.openSystem": "c4ca4238a0b923820dcc509a6f75849b",
    "business.technology": "609585a295cbae62fbbe92d81b4bd4ac",
    "business.keywords": "",
    "network.companyKey": "c379478125954196a74714441485d2b0",
    # "network.location.region": "912d59cdf1d3f551fae21f6f0062258f",
    # "network.location.country": "d8b00929dec65d422303256336ada04f",
    "network.partnership": "91471cb450b94cb668e932f217b7a5a0",
    # "network.partnership.type": "a677d68b8eb784af83b727d62c6cafa4",
    "network.supplier": "334c4a4c42fdb79d7ebc3e73b517e6f8",
    # "network.valueChainPosition": "40f84db008a8374963eecad27f26ae53",
    # "network.valueChainPosition.type": "e749ac325789686e740a70dbdb98c315",
    "network.technologyInCompanyA": "609585a295cbae62fbbe92d81b4bd4ac",
    "network.technologyInCompanyB": "",
    "network.keypersonInCompanyA": "",
    "network.keypersonInCompanyB": "",
    "network.timestamp": "",
    "network.bling": "8f0e8c881b41b9c33164a4acd5dead67",
    "network.hyperlink": "",
    "persons.firstName": "db2dcb18344057ee5528cf543b244d88",
    "persons.lastName": "7a6ac2a11d11b9d7e150de77951caedd",
    "persons.email": "",
    "persons.functions": "07e50af96c4e571e5b35ea1aa0615733",
}

unflattened = unflatten(data)
expected_output = json.dumps(unflattened, indent=4)
print(expected_output)
finish = time.perf_counter()
