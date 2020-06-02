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
    NOMOD       = 0
    NOFAIL      = 1 << 0
    EASY        = 1 << 1
    TOUCHSCREEN = 1 << 2
    HIDDEN      = 1 << 3
    HARDROCK    = 1 << 4
    SUDDENDEATH = 1 << 5
    DOUBLETIME  = 1 << 6
    RELAX       = 1 << 7
    HALFTIME    = 1 << 8
    NIGHTCORE   = 1 << 9
    FLASHLIGHT  = 1 << 10
    AUTOPLAY    = 1 << 11
    SPUNOUT     = 1 << 12
    RELAX2      = 1 << 13
    PERFECT     = 1 << 14
    KEY4        = 1 << 15
    KEY5        = 1 << 16
    KEY6        = 1 << 17
    KEY7        = 1 << 18
    KEY8        = 1 << 19
    KEYMOD      = 1 << 20
    FADEIN      = 1 << 21
    RANDOM      = 1 << 22
    LASTMOD     = 1 << 23
    KEY9        = 1 << 24
    KEY10       = 1 << 25
    KEY1        = 1 << 26
    KEY3        = 1 << 27
    KEY2        = 1 << 28
    SCOREV2     = 1 << 29

class Recalculator: # No safety checks in class.. be safe owo
    def __init__(self, gamemode: GameMode, relax: AkatsukiMode,
                 ranked: RankedStatus, limit: int, beatmap_id: int) -> None:

        self.gamemode = gamemode
        self.table = 'scores_relax' if relax else 'scores'
        self.ranked = ranked
        self.limit = limit
        self.beatmap_id = beatmap_id

        chdir(path.dirname(path.realpath(__file__)))
        self.connect_db()

    def get_map(self, beatmap_id: int) -> Optional[str]:
        filename = f'beatmaps/{beatmap_id}.osu'

        if not path.exists(filename): # cache miss
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
              {t}.50_count, {t}.misses_count, beatmaps.beatmap_id, beatmaps.ranked
            FROM {t}
            LEFT JOIN beatmaps ON beatmaps.beatmap_md5 = {t}.beatmap_md5
            LEFT JOIN users ON users.id = {t}.userid
            WHERE {t}.completed = 3
              AND {t}.play_mode = {gm}
              AND users.privileges & 1'''.format(
                  t = self.table, gm = self.gamemode)]

        if self.ranked:
            query.append(f'AND beatmaps.ranked = {self.ranked}')

        if self.beatmap_id:
            query.append(f'AND beatmaps.beatmap_id = {self.beatmap_id}')

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

            if not (filename := self.get_map(row['beatmap_id'])):
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

            if row['ranked'] == RankedStatus.LOVED:
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
    if any(i in ('-h', '--help') for i in argv):
        from sys import exit
        print('\n'.join([
            'Availaible launch flags:',
            '-g  --gamemode | specify a specific gamemode (int)',
            '-r  --relax    | specify whether to calculate vn/rx (0/1)',
            '-rs --ranked   | specify a specific ranked status (int)',
            '-l  --limit    | specify a limit for score recalculations (int)',
            '-b  --beatmap  | specify a specific beatmap id (int)'
        ]))
        exit(0)


    if len(argv) % 2 == 0:
        raise Exception('Invalid argument count.')

    # Default configuration
    gamemode = GameMode.STD
    relax = AkatsukiMode.RELAX
    ranked = 0
    limit = 0
    beatmap_id = 0

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

        elif argv[i] in ('-b', '--beatmap'):
            if not argv[i + 1].isdecimal():
                raise Exception('Beatmap must be an integer.')
            if (beatmap_id := int(argv[i + 1])) < 0:
                raise Exception('Beatmap must be >= 0 (0 for no specific ID).')

    r = Recalculator(gamemode, relax, ranked, limit, beatmap_id)
    r.recalculate_pp() # TODO: add threadcount
