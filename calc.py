from typing import Optional

from db import dbConnector
from mysql.connector import errorcode, Error as SQLError

from os import chdir, path
import struct

from enum import IntEnum

import requests
from subprocess import run, PIPE

class GameMode(IntEnum):
    STD = 0
    TAIKO = 1
    #CATCH = 2
    #MANIA = 3

class AkatsukiMode(IntEnum):
    VANILLA = 0
    RELAX = 1

class RankedStatus(IntEnum):
    #UNKNOWN = -2
    #NOT_SUBMITTED = -1
    #PENDING = 0
    #NEED_UPDATE = 1
    RANKED = 2
    #APPROVED = 3
    #QUALIFIED = 4
    LOVED = 5

class Mods(IntEnum):
    NOMOD = 0
    NOFAIL = 1
    EASY = 2 << 0
    TOUCHSCREEN = 2 << 1
    HIDDEN = 2 << 2
    HARDROCK = 2 << 3
    SUDDENDEATH = 2 << 4
    DOUBLETIME = 2 << 5
    RELAX = 2 << 6
    HALFTIME = 2 << 7
    NIGHTCORE = 2 << 8
    FLASHLIGHT = 2 << 9
    AUTOPLAY = 2 << 10
    SPUNOUT = 2 << 11
    RELAX2 = 2 << 12
    PERFECT = 2 << 13
    KEY4 = 2 << 14
    KEY5 = 2 << 15
    KEY6 = 2 << 16
    KEY7 = 2 << 17
    KEY8 = 2 << 18
    KEYMOD = 2 << 19
    FADEIN = 2 << 20
    RANDOM = 2 << 21
    LASTMOD = 2 << 22
    KEY9 = 2 << 23
    KEY10 = 2 << 24
    KEY1 = 2 << 25
    KEY3 = 2 << 26
    KEY2 = 2 << 27
    SCOREV2 = 2 << 28

class Recalculator: # No safety checks in class.. be safe owo
    def __init__(self, gamemode: GameMode, relax: AkatsukiMode,
                 ranked: RankedStatus, limit: int) -> None:

        self.gamemode = gamemode
        self.table = 'scores_relax' if relax else 'scores'
        self.ranked = ranked
        self.limit = limit

        chdir(path.dirname(path.realpath(__file__)))
        self.connect_db()

    def get_cached_map(self, beatmap_id: int) -> Optional[str]:
        filename = f'beatmaps/{beatmap_id}.osu'

        if not path.exists(filename):
            if not (r := requests.get(f'https://old.ppy.sh/osu/{beatmap_id}')):
                return # Failed to get beatmap from osu api

            with open(filename, 'w+') as f:
                f.write(r.content.decode('utf-8', 'strict'))

        return filename

    @staticmethod
    def mods_readable(mods: int) -> Optional[str]:
        if not mods:
            return

        r: List[str] = []
        if mods & Mods.NOFAIL:      r.append('NF')
        if mods & Mods.EASY:        r.append('EZ')
        if mods & Mods.HIDDEN:      r.append('HD')
        if mods & Mods.HARDROCK:    r.append('HR')
        if mods & Mods.DOUBLETIME:  r.append('DT')
        if mods & Mods.NIGHTCORE:   r.append('NC')
        if mods & Mods.HALFTIME:    r.append('HT')
        if mods & Mods.FLASHLIGHT:  r.append('FL')
        if mods & Mods.SPUNOUT:     r.append('SO')
        if mods & Mods.TOUCHSCREEN: r.append('TD')
        if mods & Mods.RELAX:       r.append('RX')
        return ''.join(r)

    def recalculate_pp(self) -> None:
        query = ['''
            SELECT
              {t}.id, {t}.mods, {t}.max_combo, {t}.100_count,
              {t}.50_count, {t}.misses_count, beatmaps.beatmap_id
            FROM {t}
            LEFT JOIN beatmaps ON beatmaps.beatmap_md5 = {t}.beatmap_md5
            LEFT JOIN users ON users.id = {t}.userid
            WHERE {t}.completed = 3
              AND {t}.play_mode = {gm}
              AND beatmaps.ranked = {r}
              AND users.privileges & 1'''.format(
                  t = self.table, gm = self.gamemode, r = self.ranked)]

        if self.limit:
            query.append(f'LIMIT {self.limit}')

        if not (res := self.db.fetchall(' '.join(query))):
            print('\x1b[0;91mFailed to find any scores.')
            return

        print(f'Found {len(res)} scores to recalculate.')

        for row in res:
            if not row['beatmap_id']:
                print('Missing beatmap in DB for score.')
                continue

            if not (filename := self.get_cached_map(row['beatmap_id'])):
                print(f'\x1b[0;91mFailed to get mapfile for {row["beatmap_id"]}\x1b[0m')
                continue

            modsFixed = row['mods'] & 6111

            command = [f'./oppai {filename}']
            command.append(f'{row["100_count"]}x100')
            command.append(f'{row["50_count"]}x50')
            command.append(f'{row["misses_count"]}m')
            command.append(f'{row["max_combo"]}x')
            command.append(f'+{self.mods_readable(row["mods"])}')
            if self.gamemode == GameMode.TAIKO:
                command.append('-taiko')

            process = run(f'{" ".join(command)} -obinary', shell = True, stdout = PIPE, stderr = PIPE)
            pp = struct.unpack('<f', process.stdout[-4:])[0]

            if not pp:
                print('Ignoring value.')
                continue

            # ensure pp is a number
            if pp != pp:
                print('\x1b[0;91mPP is NaN\x1b[0m')
                continue

            if self.ranked == RankedStatus.LOVED:
                self.db.execute(
                    f'UPDATE {self.table} SET score = %s, pp = 0.001 '
                    'WHERE id = %s', (pp, row['id']))
            else:
                self.db.execute(
                    f'UPDATE {self.table} SET pp = %s '
                    'WHERE id = %s', (pp, row['id']))

            print(f'Updated {row["id"]} to {pp:.2f}pp.')

    def connect_db(self) -> None:
        if hasattr(self, 'db'):
            return

        try:
            self.db = dbConnector.SQLPool(
                config = self.config.mysql,
                pool_size = 4)
        except SQLError as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                raise Exception('SQLError: Incorrect username/password.')
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                raise Exception('SQLError: Database does not exist')
            else:
                raise Exception(err)
        else:
            print('Successfully connected to SQL')

    @property
    def config(self) -> None:
        return __import__('config')

if __name__ == '__main__':
    from sys import argv
    if len(argv) % 2 == 0:
        raise Exception('Invalid argument count.')

    # Default configuration
    gamemode = GameMode.STD
    relax = AkatsukiMode.RELAX
    ranked = RankedStatus.RANKED
    limit = 0

    for i in range(1, len(argv), 2):
        if argv[i] in ('-g', '--gamemode'):
            if not argv[i + 1].isdecimal():
                raise Exception('Gamemode must be an integer.')
            gamemode = GameMode(int(argv[i + 1]))

        elif argv[i] in ('-r', '--relax'):
            if not argv[i + 1].isdecimal():
                raise Exception('Relax must be an integer.')
            relax = AkatsukiMode(int(argv[i + 1]))

        elif argv[i] in ('-rs', '--ranked'): # rs for rankedstatus cuz -r for relax
            if not argv[i + 1].isdecimal():
                raise Exception('Ranked status must be an integer.')
            ranked = RankedStatus(int(argv[i + 1]))

        elif argv[i] in ('-l', '--limit'):
            if not argv[i + 1].isdecimal():
                raise Exception('Limit must be an integer.')
            if (limit := int(argv[i + 1])) < 0:
                raise Exception('Limit must be >= 0 (0 for no limit).')

    r = Recalculator(gamemode, relax, ranked, limit)
    r.recalculate_pp() # TODO: add threadcount
