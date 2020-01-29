import {
    ArchiveExportProgress,
    ArchiveExportRequest, ArchiveImportProgress,
    ArchiveImportRequest,
    ArchiveObjectType,
    ArchiveObjectExportProgress,
    ArchiveService, ArchiveObjectImportProgress,
} from '..';
import {combineLatest, from, Observable, concat, defer, throwError, of} from 'rxjs';
import {concatMap, map, mapTo, tap} from 'rxjs/operators';
import {UniqueId} from '../../db/util/unique-id';
import {FileService} from '../../util/file/def/file-service';
import {TelemetryExportDelegate} from '../export/impl/telemetry-export-delegate';
import {DbService} from '../../db';
import {InjectionTokens} from '../../injection-tokens';
import {inject, injectable} from 'inversify';
import {ProducerData, TelemetryService} from '../../telemetry';
import {ZipService} from '../../util/zip/def/zip-service';
import {InvalidRequestError} from '..';
import {TelemetryImportDelegate} from '../import/impl/telemetry-import-delegate';
import {InvalidArchiveError} from '../import/error/invalid-archive-error';
import {TelemetryArchivePackageMeta} from '../export/def/telemetry-archive-package-meta';

interface ArchiveManifest {
    id: string;
    ver: string;
    ts: string;
    producer: ProducerData;
    archive: {
        count: number;
        items: {
            objectType: ArchiveObjectType
            file: string;
            contentEncoding: 'identity' | 'gzip';
            size: number;
            explodedSize: number;
        }[];
    };
}

@injectable()
export class ArchiveServiceImpl implements ArchiveService {
    private static ARCHIVE_ID = 'sunbird.data.archive';
    private static ARCHIVE_VERSION = '1.0';

    constructor(
        @inject(InjectionTokens.FILE_SERVICE) private fileService: FileService,
        @inject(InjectionTokens.DB_SERVICE) private dbService: DbService,
        @inject(InjectionTokens.TELEMETRY_SERVICE) private telemetryService: TelemetryService,
        @inject(InjectionTokens.ZIP_SERVICE) private zipService: ZipService
    ) {
    }

    private static reduceObjectProgressToArchiveObjectExportProgress(
        results: { type: ArchiveObjectType, progress: ArchiveObjectExportProgress }[]
    ): Map<ArchiveObjectType, ArchiveObjectExportProgress> {
        return results.reduce((acc, {type, progress}) => {
            acc.set(type, progress);
            return acc;
        }, new Map<ArchiveObjectType, ArchiveObjectExportProgress>());
    }

    private static reduceObjectProgressToArchiveObjectImportProgress(
        results: { type: ArchiveObjectType, progress: ArchiveObjectImportProgress }[],
    ): Map<ArchiveObjectType, ArchiveObjectImportProgress> {
        return results.reduce((acc, {type, progress}) => {
            acc.set(type, progress);
            return acc;
        }, new Map<ArchiveObjectType, ArchiveObjectImportProgress>());
    }

    export(exportRequest: ArchiveExportRequest): Observable<ArchiveExportProgress> {
        const workspacePath = `${cordova.file.externalCacheDirectory}${UniqueId.generateUniqueId()}`;
        let lastResult: ArchiveExportProgress | undefined;

        if (!exportRequest.objects.length) {
            return throwError(new InvalidRequestError('No archive objects to export'));
        }

        return concat(
            from(this.fileService.createDir(workspacePath, false)).pipe(
                concatMap(() => {
                    return combineLatest(
                        exportRequest.objects.map<Observable<{ type: ArchiveObjectType, progress: ArchiveObjectExportProgress }>>(object => {
                            switch (object.type) {
                                case ArchiveObjectType.CONTENT:
                                    // TODO
                                    throw new Error('To be implemented');
                                case ArchiveObjectType.PROFILE:
                                    // TODO
                                    throw new Error('To be implemented');
                                case ArchiveObjectType.TELEMETRY:
                                    return new TelemetryExportDelegate(
                                        this.dbService,
                                        this.fileService,
                                    ).export({ filePath: exportRequest.filePath }, { workspacePath }).pipe(
                                        map((progress) => ({ type: ArchiveObjectType.TELEMETRY, progress: progress }))
                                    );
                            }
                        })
                    );
                }),
                map((results: { type: ArchiveObjectType, progress: ArchiveObjectExportProgress }[]) => {
                    return {
                        task: 'BUILDING',
                        progress: ArchiveServiceImpl.reduceObjectProgressToArchiveObjectExportProgress(results)
                    };
                }),
                tap((results) => lastResult = results)
            ),
            defer(() => this.generateManifestFile(lastResult!, workspacePath)),
            defer(() => this.generateZipArchive(lastResult!, workspacePath))
        );
    }

    private generateZipArchive(progress: ArchiveExportProgress, workspacePath: string): Observable<ArchiveExportProgress> {
        const zipFilePath = `${cordova.file.externalCacheDirectory}archive-${new Date().toISOString()}.zip`;
        return new Observable((observer) => {
            this.zipService.zip(workspacePath, { target: zipFilePath }, [], [], () => {
                observer.next();
                observer.complete();
            }, (e) => {
                observer.error(e);
            });
        }).pipe(
            mapTo({
                ...progress,
                task: 'COMPLETE',
                filePath: zipFilePath
            })
        );
    }

    private generateManifestFile({ progress}: ArchiveExportProgress, workspacePath: string): Observable<ArchiveExportProgress> {
        return this.telemetryService.buildContext().pipe(
            map((c) => c.pdata),
            concatMap((producerData: ProducerData) => {
                const flattenedItems = Array.from(progress.entries()).reduce<{
                    file: string;
                    contentEncoding: 'identity' | 'gzip';
                }[]>((acc, [objectType, objectProgress]) => {
                    return acc.concat(objectProgress.completed);
                }, []);

                return from(this.fileService.writeFile(
                    workspacePath,
                    'manifest.json',
                    JSON.stringify({
                        id: ArchiveServiceImpl.ARCHIVE_ID,
                        ver: ArchiveServiceImpl.ARCHIVE_VERSION,
                        ts: (new Date()).toISOString(),
                        producer: producerData,
                        archive: {
                            count: flattenedItems.length,
                            items: flattenedItems
                        }
                    } as ArchiveManifest),
                    {
                        replace: true
                    }
                ));
            }),
            mapTo({
                progress,
                task: 'BUILDING_MANIFEST'
            })
        );
    }

    import(importRequest: ArchiveImportRequest): Observable<ArchiveImportProgress> {
        const workspacePath = `${cordova.file.externalCacheDirectory}${UniqueId.generateUniqueId()}`;

        if (!importRequest.objects.length) {
            return throwError(new InvalidRequestError('No archive objects to export'));
        }

        let lastResult: ArchiveImportProgress = {
            task: '',
            progress: new Map<ArchiveObjectType, ArchiveObjectImportProgress>(),
            filePath: importRequest.filePath
        };

        return concat(
            defer(() => this.extractZipArchive(lastResult, workspacePath)),
            defer(() => this.readManifestFile(lastResult, workspacePath, importRequest.objects.map(o => o.type))),
            from(this.fileService.createDir(workspacePath, false)).pipe(
                concatMap(() => {
                    return combineLatest(
                        importRequest.objects.map<Observable<{ type: ArchiveObjectType, progress: ArchiveObjectImportProgress }>>(object => {
                            switch (object.type) {
                                case ArchiveObjectType.CONTENT:
                                    // TODO
                                    throw new Error('To be implemented');
                                case ArchiveObjectType.PROFILE:
                                    // TODO
                                    throw new Error('To be implemented');
                                case ArchiveObjectType.TELEMETRY:
                                    return new TelemetryImportDelegate(
                                        this.dbService,
                                        this.fileService
                                    ).import({
                                        filePath: importRequest.filePath
                                    }, {
                                        workspacePath,
                                        items: lastResult.progress
                                            .get(ArchiveObjectType.TELEMETRY)!.pending as TelemetryArchivePackageMeta[]
                                    }).pipe(
                                        map((progress) => ({ type: ArchiveObjectType.TELEMETRY, progress: progress }))
                                    );
                            }
                        })
                    );
                }),
                map((results: { type: ArchiveObjectType, progress: ArchiveObjectImportProgress }[]) => {
                    return {
                        task: 'IMPORTING',
                        progress: ArchiveServiceImpl.reduceObjectProgressToArchiveObjectImportProgress(results)
                    };
                }),
                tap((results) => lastResult = results)
            ),
            of({
                ...lastResult,
                task: 'COMPLETE',
            })
        );
    }

    private extractZipArchive(progress: ArchiveImportProgress, workspacePath: string): Observable<ArchiveImportProgress> {
        return new Observable((observer) => {
            this.zipService.unzip(progress.filePath!, { target: workspacePath }, () => {
                observer.next();
                observer.complete();
            }, (e) => {
                observer.error(e);
            });
        }).pipe(
            mapTo({
                ...progress,
                task: 'EXTRACTING',
            })
        );
    }

    private readManifestFile(
        importProgress: ArchiveImportProgress,
        workspacePath: string,
        objectTypes: ArchiveObjectType[]
    ): Observable<ArchiveImportProgress> {
        return from(this.fileService.readAsText(workspacePath, 'manifest.json')).pipe(
            map((content) => {
                try {
                    return JSON.parse(content);
                } catch (e) {
                    throw new InvalidArchiveError('Invalid manfiest.json');
                }
            }),
            map((manifest: ArchiveManifest) => {
                return {
                    ...importProgress,
                    progress: (() => {
                        objectTypes.forEach((type) => {
                            const items = manifest.archive.items.filter((i) => i.objectType === type);

                            if (!items.length) {
                                throw new InvalidArchiveError('Nothing to import');
                            }

                            importProgress.progress.set(type, {
                                task: 'INITIALISING',
                                pending: items
                            });
                        });
                        return importProgress.progress;
                    })(),
                    task: 'VALIDATING'
                };
            })
        );
    }
}
