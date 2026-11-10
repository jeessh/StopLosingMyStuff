# Edge cases

What can go wrong, what happens, and the test that proves it.

## Power and disks

| What happens | Result | Test |
|---|---|---|
| Battery dies during a write | Half-written record is cut on restart. It was never acknowledged, so nothing that was saved is lost | `TestSyncedRecordsSurviveAnyCrash` |
| Power cut right after a new log file is made | Parent directory is fsynced before use, so the file can't vanish | `TestNewSegmentNameIsDurable` |
| fsync fails or the disk is full | Log refuses all writes, the daemon exits, launchd restarts it and recovery runs. Retrying fsync is unsafe because the OS may have dropped the data | `TestFailedSyncPoisonsLog`, `TestFailedDiskNeverExposesTheEvent` |
| Damage in the middle of the log | Everything after it is moved to `.bad` files and pulled back from the other Mac | `TestMidLogCorruptionStopsAndQuarantines`, `TestConvergesAfterBackupRestoreAndBitRot` |
| A flipped bit makes a record look unfinished | The cut bytes are still saved to a `.bad` file, and never overwrite an earlier one | `TestCutBytesAreAlwaysSavedAndNeverOverwritten` |
| Crash in the middle of a repair or a new log file | Repair is safe to run again | `TestCrashAtEveryStepKeepsAcknowledgedRecords` |
| Disk fills up mid write | Nothing partial becomes visible, and writing works again after reopening | `TestDiskFullLeavesNothingVisibleAndRecovers` |
| `kill -9` during writes | Every acknowledged note is still there and reaches the other Mac | `TestKillDuringWritesLosesNothing` |
| Daemon crashes holding the lock | The kernel releases `flock` when the process dies | same |
| A command runs while the daemon restarts | It waits for the daemon instead of failing | same |
| Two daemons on one data folder | The second one is refused | `TestDataSurvivesAndSecondDaemonIsRefused` |

## Compaction

| What happens | Result | Test |
|---|---|---|
| Power cut at any step of compaction | Nothing is removed until both snapshot copies are on disk, so either the old log or the snapshot is complete | `TestCrashAtEveryCompactionStep` |
| Disk fills up during compaction | The snapshot fails, the log is untouched and keeps working | `TestDiskFullDuringCompactionChangesNothing` |
| One snapshot copy goes bad | The other one is used and the bad copy is rewritten | `TestDamagedSnapshotCopyIsRepaired` |
| Both snapshot copies go bad | Refuses to start and changes nothing, because the log may hold events nothing else has. Moving the `snap` folder aside rebuilds from the other Mac | `TestBothCopiesDamagedRefusesToStartAndKeepsTheLog`, `TestMovingDamagedSnapshotsAsideRebuildsFromPeer` |
| Damage in an old log file the snapshot already covers | Only that file's remainder is set aside, newer entries are kept | `TestDamageInSnapshottedFileKeepsNewerEvents` |
| The other Mac hasn't confirmed some entries | Those log files stay until it does, or until it's been gone 30 days | `TestCompactionWaitsForPeers`, `TestOnlyRecentlySeenPairedPeersHoldBackCleanup` |
| A Mac comes back after its entries were removed | It merges the snapshot and keeps its own offline work, conflicts included | `TestAwayMacKeepsItsOwnWorkAndConflictsAfterSnapshot` |
| A Mac still holds a value the snapshot replaced | The old value stays gone, even though the record of replacing it was cleaned up | `TestSupersededWriteStaysDeadAfterPrunedSnapshot` |
| A brand new Mac pairs after compaction | It starts from the snapshot | `TestCompactedMacRestartsAndCatchesUpANewOne` |
| Snapshot bigger than one network frame | Sent in 4 MB pieces and checked as a whole | `TestLargeSnapshotCrossesTheNetworkInChunks` |
| Snapshot damaged in transit | Refused, nothing changes | `TestDamagedTransferIsRefused` |
| Snapshot disagrees with history this Mac has | Refused and shown in `sls doctor` | `TestSnapshotFromAnotherHistoryIsRefused` |
| Peer compacted past a writer this Mac rejected | Sync from that peer stops with a clear error and doesn't download the snapshot until a restart clears the rejection | `TestRejectedWriterIsNotDownloadedAgain` |
| Entry from a newer version this build can't read | Kept whole in the snapshot and passed along, and can't revive anything once a Mac that understands it has moved on | `TestEventsFromNewerBuildsSurviveCompaction`, `TestStaleRelayedEventCannotReviveAReplacedValue` |
| `sls status` reads while the daemon compacts | The read notices and starts over | `TestReadOnlySeesCompactedStore` |
| Any mix of the above across 2 or 3 Macs | Final state matches replaying every acknowledged event | `TestCompactionNeverChangesTheResult` |

## Syncing

| What happens | Result | Test |
|---|---|---|
| Connection drops mid sync | Next attempt asks again from where it got to | `TestNoAcknowledgedEventIsLost` |
| Duplicate or partial batches | Duplicates ignored, order enforced per writer | same |
| Moving from Ethernet to home Wi-Fi | Every address is tried at once, a dead one costs nothing | `TestPullOverTLSUsesFirstWorkingAddress` |
| Laptop sleeps mid connection | Every read and write has a deadline | deadlines in `replica/net.go` |
| Restored from an old backup | Gets newer work back from the other Mac, and writes under a new ID so nothing collides | `TestRestoredBackupGetsNewerWorkBack` |
| Wrong date on one Mac | Can't win a conflict or pull the other clock far forward | `TestWrongClockCannotHideAConflict`, `TestObserveIgnoresFarFuture` |
| Both Macs change the same thing offline | Both are kept and shown as a conflict | `TestSurvivesKillAndOfflineConflict` |
| A summary lands during a conflict | It can only update its own side | `TestSummaryCannotEraseAnOpenConflict` |
| An old session is summarized late | It can't replace a newer summary | `TestOlderSummaryCannotReplaceNewerOne` |
| An event was altered | Hash fails, that writer is stopped, `sls doctor` reports it | `TestTamperedEventIsQuarantined` |
| Unknown device connects | Dropped during the TLS handshake | `TestStrangerIsRejected`, `TestWrongServerIsRejected` |
| Idle connections try to use up the server | Handshakes have their own limit and timeout | `TestIdleStrangersDoNotBlockPeers` |
| A paired Mac sends a nonsense request | Answered with nothing instead of crashing | `TestHostileVectorCannotCrashTheServer` |
| Very slow connection | Deadlines reset as data moves, so a big batch can still finish | chunked frames in `replica/net.go` |
| Macs on different versions | Protocol mismatch stops sync. New event types pass through older builds | `TestProtocolMismatchStopsSync`, `TestUnknownOpsAreIgnored` |

## Sessions and transcripts

| What happens | Result | Test |
|---|---|---|
| Session breaks mid generation | Only whole lines are read. The half-written one is picked up if it ever finishes | `TestHalfWrittenLineWaitsForTheRest` |
| A tool started and never returned | Shown as unfinished, outcome unknown, verify before repeating | `TestToolWithNoResultStaysPending`, `TestCondition` |
| Garbage line | Counted and skipped. `sls doctor` flags it if many are bad, in case the format changed | `TestGarbageLineIsCountedNotFatal` |
| Huge line | Skipped without running out of memory | `TestOversizedLineIsSkipped` |
| Transcript replaced | Read again from the start | `TestReplacedFileIsReadFromTheStart` |
| Daemon restarts | Continues from a saved position, and repeats are harmless | `TestCursorSurvivesRestartWithoutRereading` |
| First install with months of history | Only the last 14 days are imported | `TestOldTranscriptsAreSkippedUntilTheyChange` |
| Subagents and harness messages | Don't count as your work | `TestSubagentsAndHarnessTextAreIgnored` |
| Summaries triggering more summaries | Hooks off, no saved session, and an internal flag | `TestCLIMarksItselfInternalAndStaysClean`, `TestHookIsSilentWithNoDataAndWhenInternal` |
| Summary call fails | Retried with a gap, gives up after 5 tries, facts are unaffected | `TestCLIFailureIsAnError` |
| One odd field from a tool | Only that field is skipped, not the whole message | `TestOddToolInputKeepsTheRestOfTheMessage` |
| Pressing Esc on a response | Doesn't count as a new prompt | `TestInterruptMarkerIsNotAPrompt` |
| Secrets in commands or prompts | Removed before reaching the log | `TestRedact`, `TestRedactCommandLineForms`, `TestNotesAreRedacted` |
| Instructions hidden in a transcript | Every field is flattened to one line and fenced as data, so it can't pose as a real line | `TestTranscriptTextCannotPoseAsBriefingLines`, `TestTranscriptCannotCloseItsTag` |

## Projects and hooks

| What happens | Result | Test |
|---|---|---|
| Same repo at different paths | Identified by git remote, ssh and https match | `TestSameRepoGetsSameIDAtDifferentPaths`, `TestNormalizeRemote` |
| Worktrees and subfolders | Count as the same project | `TestSubdirAndWorktreeResolveToTheProject` |
| Folders without git, like game mods | `sls track` adds a small ID file that survives copying | `TestTrackedFolderIdentitySurvivesACopy` |
| Paths in a briefing | Relative to the project, so they work on the other Mac | `TestHandoffAcrossMachines` |
| Git times out, or macOS blocks folder access | The session waits and retries instead of being filed under the wrong project | `TestGitFailureIsNotTreatedAsNoRepo` |
| A helper process hangs | It's killed on a timeout, even if its children hold the pipes open | `TestHungGitChildDoesNotHangTheCaller` |
| Daemon is down when a session starts | Hook reads the files directly and is capped at 4 seconds | `TestHookIsSilentWithNoDataAndWhenInternal` |
| Existing Claude Code settings | Only adds entries, keeps a backup, refuses invalid JSON, keeps symlinks | `TestInstallHooksKeepsExistingSettings`, `TestInstallHooksRefusesBrokenSettings`, `TestInstallHooksKeepsASymlink` |

## Not handled yet

- Deleted list items are remembered forever, so re-reading an old transcript can't bring them back. They're small, but compaction can't remove them.
- Code and non-git files aren't copied, only tracked. Git still owns your code.
- A live conversation doesn't move between Macs. The next session starts with the context instead.
- If a Mac dies before syncing, the other one has to wait for it to come back for the last few events.
- A Bash command's real effects can't always be known after a crash, so the briefing says unknown.
