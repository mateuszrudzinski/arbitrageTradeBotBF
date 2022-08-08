from binance import Client
import pandas as pd
import numpy as np
import networkx as nx
import math
from collections import defaultdict
"""
Połączenie API
"""
client = Client('KEYXDsqrE6Moq6VhnuESU8el8zTphO13Po33TuCpyNB4KZAozg7cVZwOtZx83Nvz45h',
                '7lPenaI4xwZSJv1gSoiFMtWeJvhtTQ9Xh0n7gvdTrISe66XP9OVxAqRSu5sQWb5y')
while(1):
    """
    Definiowanicje zmiennych przechowujacych dane z giełdy ( wartość ask i bid ), listing aktualnie istniejących par symboli
    oraz deklarowanie tablic znaków do przechowywania ich przed obróbką ) 
    """
    beforeSortquoteAsset = []
    beforeSortBaseAssets = []
    exchangeinfo = client.get_exchange_info()
    tickers = client.get_orderbook_tickers()
    """
    Przypisanie do tablic smyboli rozbitych na dwa człony ( znak główny oraz znak dodatkowy przykład BTCETH, BTC - głowny, ETH - dodatkowyh)
    """
    for s in exchangeinfo['symbols']:
        beforeSortBaseAssets.append(s.get('baseAsset'))
        beforeSortquoteAsset.append(s.get('quoteAsset'))


    def getBidValue(baseAsset, quoteAsset):
        """
        Funkcja służąca pobraniu wartosci BID dla danego symbolu, złożonego z głównego i dodatkowego znaku podanego w argumentach
        :param baseAsset: główny znak
        :type baseAsset: str
        :param quoteAsset: dodatkowy znak
        :type quoteAsset: str
        :return: Wartość BID dla danego symbolu
        """
        fullSymbol = baseAsset + quoteAsset
        for i in tickers:
            if fullSymbol == i.get('symbol') and float(i.get('bidPrice')) > 0:
                bidPrice = i.get('bidPrice')
                return bidPrice




    def getAskValue(baseAsset, quoteAsset):
        """
        Funkcja służąca pobraniu wartosci ASK dla danego symbolu, złożonego z głównego i dodatkowego znaku podanego w argumentach
        :param baseAsset: główny znak
        :type baseAsset: str
        :param quoteAsset: dodatkowy znak
        :type quoteAsset: str
        :return: Wartość ASK dla danego symbolu
        """
        fullSymbol = baseAsset + quoteAsset
        for i in tickers:
            if fullSymbol == i.get('symbol') and float(i.get('askPrice')) > 0:
                askPrice = i.get('askPrice')
                return askPrice




    def sortBaseAssetsList():
        """
        Funkcja sortująca mająca na celu wyizolowanie tylko tych znaków głównych które posiadają znaczną ilość połączeń tak aby umożliwić
        jak najwięcej połączeń w grafie, dodatkowo sortowanie alfabetycznie oraz elimnuje powtórzenia
        :return: Posortowana lista głównych znaków
         """
        afterSortBaseAssets = []
        for x in beforeSortBaseAssets:
            if beforeSortBaseAssets.count(x) > 10:
                afterSortBaseAssets.append(x)
        afterSortBaseAssets = list(set(afterSortBaseAssets))
        afterSortBaseAssets.sort()
        return afterSortBaseAssets



    def sortQuoteAssetsList():
        """
        Funkcja sortująca mająca na celu wyizolowanie tylko tych znaków dodatkowych które posiadają znaczną ilość połączeń tak aby umożliwić
        jak najwięcej połączeń w grafie, dodatkowo elimnuje powtórzenia
        :return: Posortowana lista dodatkowych znaków
        """
        afterSortQuoteAssets = []
        for y in beforeSortquoteAsset:
            if beforeSortquoteAsset.count(y) > 20:
                afterSortQuoteAssets.append(y)
        afterSortQuoteAssets = list(set(afterSortQuoteAssets))
        return afterSortQuoteAssets




    def createAdjMatrix():
        """
        Funkcja tworząca dataframe w oparciu na posortowane listy znaków oraz wartosci ASK i BID ich połączeń
        :return: Plik csv zawierający dataframe
        """

        baseAssetsList = sortBaseAssetsList()
        quoteAssetsList = sortQuoteAssetsList()
        listAll = list(set(baseAssetsList).union(set(quoteAssetsList)))
        df = pd.DataFrame(columns=listAll, index=listAll)
        for p1 in baseAssetsList:
            for p2 in quoteAssetsList:
                try:
                    if getBidValue(p1, p2) is not None and getAskValue(p1, p2) is not None:
                        df[p1][p2] = getBidValue(p1, p2)
                        df[p2][p1] = 1 / float(getAskValue(p1, p2))
                except KeyError:
                    continue
        df.to_csv("output.csv")


    def bellmanFordNegativeCycles(g, s):
        """
        :param g: graf
        :type g: networkx weighted DiGraph
        :param s: wierzchołek startowy
        :return: wszystkie negatywne cykle osiągalne z wierzchołka startowego
        """
        n = len(g.nodes())
        d = defaultdict(lambda: math.inf)  # dictionary kosztów dojścia ( domyślna wartość to nieskończoność )
        p = defaultdict(lambda: -1)  # dictionary poprzedników ( domyślna wartość to -1 ponieważ nie ma takiego wierzchłoka w grafie co oznacza brak poprzednika)
        d[s] = 0
        # Każdy obieg pętli ustala koszt dojścia do wierzchoiłka grafu, wierzchołek startowy ma koszt 0, pozostaje nam więc ustalenie kosztu dla n-1 wierzchołków )
        for _ in range(n - 1):
            for u, v in g.edges():
                # Bellman-Ford relaksacja krawędzi
                weight = g[u][v]["weight"]
                if d[u] + weight < d[v]:
                    d[v] = d[u] + weight
                    p[v] = u  # update poprzednika

        # Znajdź cykle ujemne jeśli istnieją
        allCycles = []
        seen = defaultdict(lambda: False)

        for u, v in g.edges():
            weight = g[u][v]["weight"]
            # Jeśli możemy dalej relaksować krawędzie, w ścieżkach musi znajdować się cykl ujemny
            if seen[v]:
                continue

            if d[u] + weight < d[v]:
                cycle = []
                x = v
                while True:
                    # Przechodzimy po poprzednikach dopóki nie znajdziemy cyklu
                    seen[x] = True
                    cycle.append(x)
                    x = p[x]
                    if x == v or x in cycle:
                        break

                idx = cycle.index(x)
                cycle.append(x)
                allCycles.append(cycle[idx:][::-1])
        return allCycles


    def allNegativeCycles(g):
        """
        Zbiera wszystkie cykle ujemne przez wywoływanie Bellmana-Forda na każdym z wierzchołków
        :param g: graf
        :type g: networkx weighted DiGraph
        :return: lista cyklów ujemnych
        """
        allPaths = []
        for v in g.nodes():
            allPaths.append(bellmanFordNegativeCycles(g, v))
        flatten = lambda l: [item for sublist in l for item in sublist]
        return [list(i) for i in set(tuple(j) for j in flatten(allPaths))]


    def calculateArb(cycle, g, verbose=True):
        """
        Dla podanego cyklu ujemnego oblicza i drukuje arbitraż
        :param cycle: cykl ujemny
        :param g: graph
        :type g: networkx weighted DiGraph
        :param verbose: czy drukować scieżkę i arbitraż
        :return: Procentowa wartość arbitrażu

        """
        total = 0
        for (p1, p2) in zip(cycle, cycle[1:]):
            total += g[p1][p2]["weight"]
        arb = np.exp(-total) - 1
        if verbose:
            print("Path:", cycle)
            print(f"{arb * 100:.2g}%\n")
        return arb


    def find_arbitrage(filename="output.csv", findAll=False, sources=None):
        """
        Przeszukuje plik podany w argumencie pod kątem okazji na arbitraż
        :param filename: nazwa pliku
        :param findAll: szuka wszystkich ścieżek jesli TRUE, jeśli FALSE należy podać punkty startowe
        :return: lista negatywnych cykli
        :rtype: str list
        """
        # Odczytuje Dataframe i przekształca do ujemnych logarytmów tak by można było uzyć Bellmana-Forda
        # Ujemne cykle odpowiadają zatem okazją arbitrażowym
        # Transpozycja log_df tak by graf miał to samo API co dataframe
        df = pd.read_csv(filename, header=0, index_col=0)
        g = nx.DiGraph(-np.log(df).fillna(0).T)

        if nx.negative_edge_cycle(g):
            print("ZNALEZIONO ARBITRAZ\n" + "=" * 15 + "\n")

            if findAll:
                uniqueCycles = allNegativeCycles(g)
            else:
                allPaths = []
                for s in sources:
                    allPaths.append(bellmanFordNegativeCycles(g, s))
                flatten = lambda l: [item for sublist in l for item in sublist]
                uniqueCycles = [list(i) for i in set(tuple(j) for j in flatten(allPaths))]

            for p in uniqueCycles:
                calculateArb(p, g)
            return uniqueCycles

        else:
            print("Brak okazji arbitrazu")
            return None


    createAdjMatrix()
    find_arbitrage(findAll=True)
