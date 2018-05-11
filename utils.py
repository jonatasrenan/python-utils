import json
import pprint as pp
pprint = pp.PrettyPrinter(indent=4).pprint


def to_csv(dics, filename, keys=None):
    """
    Create a CSV from a dictionary list
    :param dics: dictionary list
    :param filename: output filename
    :param keys: Optional, subset of keys. Default is all keys.
    :return: None
    """
    if not keys:
        keys = sorted(set().union(*(d.keys() for d in dics)))

    import csv
    with open(filename, 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(dics)


def csv(filename):
    """
    Read CSV and make a dictionary list
    :param filename: csv
    :return: dictionary list
    """
    import csv
    return list(csv.DictReader(open(filename)))


default_set = set


def set(seq=()):
    """
    Sobrescreve função set para aceitar dicionários
    :param seq: Lista de dicionários
    :return: Lista de dicionários únicos
    """
    if type(seq) is list and type(seq[0]) is dict:
        jsons = [json.dumps(d, sort_keys=True) for d in seq]
        jsons_set = list(set(jsons))
        res = [json.loads(j) for j in jsons_set]
        return res
    else:
        return default_set(seq)


def remove_duplicates(lista):
    """
    Remove duplicados de uma lista
    :param lista:
    :return:
    """
    return list(set(lista))


def flat(l):
    """
    Aplaina uma lista de lista
        ex: [[1,2,3],[4,5,6],[7,8,9]] = [1,2,3,4,5,6,7,8,9]
    :param l: lista de lista
    :return: lista
    """
    return [item for sublist in l for item in sublist]


def tmap(func, args, workers=16):
    """
    Redefinição da função map, multithread, aguarda threads no final e retorna resultado expandido em lista.
    :param func: função
    :param args: lista
    :param workers: número de threads máximo
    :return: resultado do mapeamento de fn em l expandido em lista
    """

    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(workers) as ex:
        res = ex.map(func, args)
        ex.shutdown(wait=True)

    return list(res)


def pmap(func, args, workers=8):
    """
    Redefinição da função map, multiprocessos, aguarda processos no final e retorna resultado expandido em lista.
    :param func: função
    :param args: lista
    :param workers: número de processos máximo
    :return: resultado do mapeamento de fn em l expandido em lista
    """

    if workers == 1:
        return list(map(func, args))

    import concurrent.futures
    with concurrent.futures.ProcessPoolExecutor(workers) as ex:
        res = ex.map(func, args)
        ex.shutdown(wait=True)
    return list(res)


def max(a, b):
    """
    Função max(1,3)=3
    :param a: Primeira parcela
    :param b: Segunda parcela
    :return: Retorna parcela maior
    """
    return a if a > b else b


def digitos(txt):
    """
    Função digitos('asdf123123') = '123123'
    :param txt: Texto inicial
    :return: Saída somente com dígitos
    """
    return ''.join([c for c in txt if c.isdigit()])


def normalize_unicode(data):
    import unicodedata
    return str(unicodedata.normalize('NFKD', data).encode('ASCII', 'ignore').decode('ascii'))


def stringify(node):
    """
    Dado um nó HTML concatena todos os textos visíveis incluindo os nós filhos e retorna um único string
    :param node: Nó pai
    :return: String
    """
    from itertools import chain
    parts = ([node.text] + list(chain(*([c.text, c.tail] for c in node.getchildren()))) + [node.tail])
    parts = list(filter(None, parts))
    parts = [str(i).strip() for i in parts]
    parts = [i for i in parts if i]
    return ''.join(filter(None, parts))


def print_name(func):
    def echo_func(*func_args, **func_kwargs):
        print('{} Call'.format(func.__name__))
        return func(*func_args, **func_kwargs)
    return echo_func
