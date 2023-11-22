import {defer, from, Observable, of, throwError, zip} from 'rxjs';
import {catchError, map, mapTo, mergeMap, reduce, tap} from 'rxjs/operators';
import { ContentAccessEntry, ContentFeedbackEntry, ContentMarkerEntry } from '../../content/db/schema';
import { KeyValueStoreEntry } from '../../key-value-store/db/schema';
import { PlayerConfigEntry } from '../../player/db/schema';
import { SearchHistoryEntry } from '../../util/search-history/db/schema';
import {DbService} from '../../db';
import { GroupProfileEntry, LearnerAssessmentsEntry,LearnerSummaryEntry, ProfileEntry, UserEntry } from '../db/schema';

export class DeleteProfileDataHandler {

    constructor(
        private dbService: DbService
    ) {
    }

    delete(uid: string ): Observable<boolean> {
        return defer(() => of(this.dbService.beginTransaction())).pipe(
            mergeMap(() => {
                return zip(
                    this.dbService.delete({
                        table: ProfileEntry.TABLE_NAME,
                        selection: `${ProfileEntry.COLUMN_NAME_UID} = ?`,
                        selectionArgs: [uid]
                    }),
                    this.dbService.delete({
                        table: ContentMarkerEntry.TABLE_NAME,
                        selection: `${ContentMarkerEntry.COLUMN_NAME_UID} = ?`,
                        selectionArgs: [uid]
                    }),
                    this.dbService.delete({
                        table: ContentAccessEntry.TABLE_NAME,
                        selection: `${ContentAccessEntry.COLUMN_NAME_UID} = ?`,
                        selectionArgs: [uid]
                    }),
                    this.dbService.delete({
                        table: LearnerAssessmentsEntry.TABLE_NAME,
                        selection: `${LearnerAssessmentsEntry.COLUMN_NAME_UID} = ?`,
                        selectionArgs: [uid]
                    }),
                    this.dbService.delete({
                        table: LearnerSummaryEntry.TABLE_NAME,
                        selection: `${LearnerSummaryEntry.COLUMN_NAME_UID} = ?`,
                        selectionArgs: [uid]
                    }),
                    this.dbService.delete({
                        table: ContentFeedbackEntry.TABLE_NAME,
                        selection: `${ContentFeedbackEntry.COLUMN_NAME_UID} = ?`,
                        selectionArgs: [uid]
                    }),
                    this.dbService.delete({
                        table: SearchHistoryEntry.TABLE_NAME,
                        selection: `${SearchHistoryEntry.COLUMN_NAME_USER_ID} = ?`,
                        selectionArgs: [uid]
                    }),
                    this.dbService.delete({
                        table: GroupProfileEntry.TABLE_NAME,
                        selection: `${GroupProfileEntry.COLUMN_NAME_UID} = ?`,
                        selectionArgs: [uid]
                    }),
                    this.dbService.delete({
                        table: UserEntry.TABLE_NAME,
                        selection: `${UserEntry.COLUMN_NAME_UID} = ?`,
                        selectionArgs: [uid]
                    }),
                    this.dbService.delete({
                        table: PlayerConfigEntry.TABLE_NAME,
                        selection: `${PlayerConfigEntry.COLUMN_NAME_USER_ID} = ?`,
                        selectionArgs: [uid]
                    }),
                    this.dbService.execute(`DELETE FROM ${KeyValueStoreEntry.TABLE_NAME}     
                                            WHERE ${KeyValueStoreEntry.COLUMN_NAME_KEY} 
                                            LIKE '%%${uid}%%'`)
                ).pipe(
                    mapTo(true)
                );
            }),
            tap(() => {
                this.dbService.endTransaction(true);
            }),
            catchError((e) => {
                console.log(e);
                this.dbService.endTransaction(false);
                return of(false);
            })
        );
    }
}