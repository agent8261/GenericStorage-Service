package edu.umich.imlc.mydesk.test.service;

import java.io.File;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.umich.imlc.mydesk.MyDeskProtocolBuffer.FileMetaData_PB;
import edu.umich.imlc.mydesk.MyDeskProtocolBuffer.FileMetaData_ShortInfo_PB;
import edu.umich.imlc.mydesk.cloud.android.auth.LoginTask;
import edu.umich.imlc.mydesk.cloud.client.network.NetworkOps;
import edu.umich.imlc.mydesk.cloud.client.utilities.Util;
import edu.umich.imlc.mydesk.test.common.GenericContract;
import edu.umich.imlc.mydesk.test.common.GenericContract.BackendConflictColumns;
import edu.umich.imlc.mydesk.test.common.GenericContract.GenericURIs;
import edu.umich.imlc.mydesk.test.common.GenericContract.MetaData;
import edu.umich.imlc.mydesk.test.common.GenericContract.MetaDataColumns;
import edu.umich.imlc.mydesk.test.common.GenericContract.MetaDataProjections;
import edu.umich.imlc.mydesk.test.common.Utils;
import edu.umich.imlc.protocolbuffer.general.ProtocolBufferTransport.Date_PB;

import android.accounts.Account;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.app.Service;
import android.app.TaskStackBuilder;
import android.content.AbstractThreadedSyncAdapter;
import android.content.ContentProviderClient;
import android.content.ContentProviderOperation;
import android.content.ContentResolver;
import android.content.Context;
import android.content.Intent;
import android.content.SyncResult;
import android.database.Cursor;
import android.net.Uri;
import android.os.Bundle;
import android.os.IBinder;
import android.os.RemoteException;
import android.support.v4.app.NotificationCompat;
import android.util.Log;

public class GenericSyncService extends Service
{
  private static final String TAG = "GenericSync";
  private static GenericSyncAdapter syncAdapter = null;
  private static final int NOTIFICATION_CONFLICT = 1010;
  private static final int NOTIFICATION_SYNC = 1011;

  @Override
  public IBinder onBind(Intent arg0)
  {
    Util.printMethodName(TAG);
    return getSyncAdapter().getSyncAdapterBinder();
  }

  private GenericSyncAdapter getSyncAdapter()
  {
    Util.printMethodName(TAG);
    if( syncAdapter == null )
    {
      syncAdapter = new GenericSyncAdapter(this);
    }
    return syncAdapter;
  }

  public static Date translateDate(Date_PB dPB)
  {
    Utils.printMethodName(TAG);
    Calendar c = Calendar.getInstance();
    c.set(dPB.getYear() - 1900, dPB.getMonth() - 1, dPB.getDay(), dPB.getTime()
        .getHours(), dPB.getTime().getMinutes(), dPB.getTime().getSeconds());
    return c.getTime();
  }

  public static void displayConflictNotification(Context context)
  {
    Utils.printMethodName(TAG);
    NotificationCompat.Builder mBuilder = new NotificationCompat.Builder(
        context).setContentTitle("MyDesk Conflict")
        .setContentText("Tap to resolve conflict")
        .setSmallIcon(android.R.drawable.stat_notify_sync_noanim)
        .setOngoing(true);
    // Creates an explicit intent for an Activity in your app
    Intent resultIntent = new Intent(context, ConflictActivity.class);

    // The stack builder object will contain an artificial back stack for the
    // started Activity.
    // This ensures that navigating backward from the Activity leads out of
    // your application to the Home screen.
    TaskStackBuilder stackBuilder = TaskStackBuilder.create(context);
    // Adds the back stack for the Intent (but not the Intent itself)
    stackBuilder.addParentStack(ConflictActivity.class);
    // Adds the Intent that starts the Activity to the top of the stack
    stackBuilder.addNextIntent(resultIntent);
    PendingIntent resultPendingIntent = stackBuilder.getPendingIntent(0,
        PendingIntent.FLAG_UPDATE_CURRENT);
    mBuilder.setContentIntent(resultPendingIntent);
    NotificationManager mNotificationManager = (NotificationManager) context
        .getSystemService(Context.NOTIFICATION_SERVICE);
    // mId allows you to update the notification later on.
    mNotificationManager.notify(NOTIFICATION_CONFLICT, mBuilder.build());
  }// displayConflictNotification

  public static NotificationCompat.Builder displaySyncNotification(
      Context context)
  {
    Utils.printMethodName(TAG);
    NotificationCompat.Builder mBuilder = new NotificationCompat.Builder(
        context).setContentTitle("MyDesk Service")
        .setContentText("Sync in progress")
        .setSmallIcon(android.R.drawable.stat_notify_sync)
        .setProgress(0, 0, true).setOngoing(true);

    NotificationManager mNotificationManager = (NotificationManager) context
        .getSystemService(Context.NOTIFICATION_SERVICE);
    // mId allows you to update the notification later on.
    mNotificationManager.notify(NOTIFICATION_SYNC, mBuilder.build());
    return mBuilder;
  }// displaySyncNotification

  public static void updateSyncNotification(Context context,
      NotificationCompat.Builder mBuilder, String contentText)
  {
    mBuilder.setContentText(contentText);
    NotificationManager mNotificationManager = (NotificationManager) context
        .getSystemService(Context.NOTIFICATION_SERVICE);
    // mId allows you to update the notification later on.
    mNotificationManager.notify(NOTIFICATION_SYNC, mBuilder.build());
  }

  public static void dissmissNotification(Context context, int id)
  {
    NotificationManager mNotificationManager = (NotificationManager) context
        .getApplicationContext().getSystemService(Context.NOTIFICATION_SERVICE);
    mNotificationManager.cancel(id);
  }

  private static class GenericSyncAdapter extends AbstractThreadedSyncAdapter
  {

    public GenericSyncAdapter(Context context)
    {
      super(context, true);
      Utils.printMethodName(TAG);
    }

    @Override
    public void onPerformSync(Account account, Bundle extras, String authority,
        ContentProviderClient provider, SyncResult syncResult)
    {
      Util.printMethodName(TAG);
      Log.d(TAG, "Starting sync for " + account.name);
      String accountName = getContext().getSharedPreferences(
          GenericContract.SHARED_PREFS, 0).getString(
          GenericContract.PREFS_ACCOUNT_NAME, null);
      if( accountName == null || !accountName.equals(account.name) )
      {
        // no / not current user
        Log.d(TAG, "The requested sync account " + account.name
            + " is not the currently logged in account");
        ContentResolver.setSyncAutomatically(account,
            GenericContract.AUTHORITY, false);
        Log.d(TAG, "Sync finsihed for " + account.name);
        return;
      }
      NotificationCompat.Builder mBuilder = displaySyncNotification(getContext()
          .getApplicationContext());
      try
      {
        new LoginTask(getContext().getApplicationContext(), account.name)
            .doLogin();

        SyncTodos todos = new SyncTodos(getAndLockMetaDatas(account, provider),
            NetworkOps.getListMetaData(), syncResult, account, mBuilder, getContext().getApplicationContext());

        ArrayList<ContentProviderOperation> operationList = new ArrayList<ContentProviderOperation>();

        todos.addUnlockUnrelatedOperations(operationList);
        provider.applyBatch(operationList);
        operationList.clear();

        todos.addPullOperations(operationList);
        provider.applyBatch(operationList);
        operationList.clear();

        todos.addNewFilesOperations(operationList, account.name);
        provider.applyBatch(operationList);
        operationList.clear();

        todos.addcreateOperations(operationList);
        provider.applyBatch(operationList);
        operationList.clear();

        todos.addPushOperations(operationList);
        provider.applyBatch(operationList);
        operationList.clear();

        todos.addConflictOperations(operationList);
        provider.applyBatch(operationList);
        operationList.clear();

        provider.update(
            GenericURIs.URI_FILES.buildUpon()
                .appendQueryParameter(MetaDataColumns.OWNER, account.name)
                .appendQueryParameter(GenericContract.UNLOCK_FILE, "true")
                .build(), null, null, null);
      }
      catch( Exception e )
      {
        ++syncResult.stats.numIoExceptions;
        e.printStackTrace();
      }
      finally
      {
        Log.d(TAG, "Sync finsihed for " + account.name);
        dissmissNotification(getContext().getApplicationContext(),
            NOTIFICATION_SYNC);
      }
    }

    private Map<String, MetaData> getAndLockMetaDatas(Account account,
        ContentProviderClient provider) throws RemoteException
    {
      Util.printMethodName(TAG);
      provider.update(
          GenericURIs.URI_FILES.buildUpon()
              .appendQueryParameter(MetaDataColumns.OWNER, account.name)
              .appendQueryParameter(GenericContract.LOCK_FILE, "true").build(),
          null, null, null);
      Cursor c = provider.query(GenericURIs.URI_FILES.buildUpon()
          .appendQueryParameter(MetaDataColumns.OWNER, account.name).build(),
          MetaDataProjections.METADATA, null, null, null);
      try
      {
        Map<String, MetaData> result = new HashMap<String, GenericContract.MetaData>();
        if( c.moveToFirst() )
        {
          do
          {
            MetaData m = new MetaData(c);
            result.put(m.fileId(), m);
          } while( c.moveToNext() );
        }
        return result;
      }
      finally
      {
        c.close();
      }
    }
  }

  private static class SyncTodos
  {
    private final Account syncAccount;
    private ArrayList<MetaData> untouched;
    private ArrayList<MetaData> conflicts;
    private ArrayList<MetaData> toCreate;
    private ArrayList<MetaData> toPush;
    private ArrayList<MetaData> toPull;
    private ArrayList<FileMetaData_ShortInfo_PB> newFiles;
    private SyncResult mSyncResult;
    private NotificationCompat.Builder mBuilder;
    private Context mContext;
    
    public SyncTodos(Map<String, MetaData> local_,
        List<FileMetaData_ShortInfo_PB> backend, SyncResult syncResult_,
        Account account_, NotificationCompat.Builder notBuilder_, Context c_)
    {
      Util.printMethodName(TAG);
      syncAccount = account_;
      mSyncResult = syncResult_;
      mBuilder = notBuilder_;
      mContext = c_;
      Log.d(TAG, "local:\n" + local_);
      Log.d(TAG, "Backend:\n" + backend);
      untouched = new ArrayList<GenericContract.MetaData>();
      conflicts = new ArrayList<GenericContract.MetaData>();
      toCreate = new ArrayList<GenericContract.MetaData>();
      toPush = new ArrayList<GenericContract.MetaData>();
      toPull = new ArrayList<MetaData>();
      newFiles = new ArrayList<FileMetaData_ShortInfo_PB>();
      for( FileMetaData_ShortInfo_PB b : backend )
      {
        if( !local_.containsKey(b.getFileID()) )
        {
          ++mSyncResult.stats.numInserts;
          newFiles.add(b);
          continue;
        }
        ++mSyncResult.stats.numEntries;
        MetaData m = local_.remove(b.getFileID());
        if( m.conflict() )
        {
          // don't touch conflicted files, but don't unlock them since we will
          // be touching them after this
          continue;
        }
        if( m.dirty() )
        {
          if( m.sequenceNumber() <= b.getSequenceNumber() )
          {
            ++mSyncResult.stats.numConflictDetectedExceptions;
            conflicts.add(m);
            continue;
          }
          else
          {
            toPush.add(m);
            continue;
          }
        }
        else
        {
          if( m.sequenceNumber() < b.getSequenceNumber() )
          {
            ++mSyncResult.stats.numUpdates;
            toPull.add(m);
            continue;
          }
        }
        untouched.add(m);
        ++mSyncResult.stats.numSkippedEntries;
      }

      // any files remaining means that they don't exist on the backend, so
      // conflict is impossible, but exclude them when there is a local
      // conflict.
      for( MetaData m : local_.values() )
      {
        ++mSyncResult.stats.numEntries;
        if( m.dirty() && !m.conflict() )
        {
          toCreate.add(m);
          continue;
        }
        untouched.add(m);
        ++mSyncResult.stats.numSkippedEntries;
      }
      Log.d(TAG, "conflicts:\n" + conflicts);
      Log.d(TAG, "toCreate:\n" + toCreate);
      Log.d(TAG, "toPush:\n" + toPush);
      Log.d(TAG, "toPull:\n" + toPull);
      Log.d(TAG, "newFiles:\n" + newFiles);
      Log.d(TAG, "untouched:\n" + untouched);
    }

    public void addPullOperations(
        ArrayList<ContentProviderOperation> operationList)
    {
      Util.printMethodName(TAG);
      for( MetaData m : toPull )
      {
        try
        {
          updateSyncNotification(mContext, mBuilder, "Updating: " + m.fileName());
          File newFile = new File(Utils.createRandomFileUri(mContext.getFilesDir())
              .getPath());
          FileMetaData_PB m_pb = NetworkOps.getFileMetaData(m.fileId());
          long newSeq = NetworkOps.getFile(m.fileId(), newFile);
          ContentProviderOperation.Builder b = ContentProviderOperation
              .newUpdate(GenericURIs.URI_FILES
                  .buildUpon()
                  .appendPath(m.fileId())
                  .appendQueryParameter(GenericContract.CALLER_IS_SYNC_ADAPTER,
                      "true").build());
          b.withValue(GenericContract.KEY_NEW_FILE, newFile.toString())
              .withValue(GenericContract.KEY_UPDATE_OLD_SEQUENCE,
                  m.sequenceNumber())
              .withValue(MetaDataColumns.SEQUENCE, newSeq)
              .withValue(
                  MetaDataColumns.TIME,
                  DateFormat.getDateTimeInstance().format(
                      translateDate(m_pb.getLastUpdated())));
          operationList.add(b.build());
        }
        catch( Exception e )
        {
          ++mSyncResult.stats.numIoExceptions;
          e.printStackTrace();
        }
      }
    }

    public void addNewFilesOperations(
        ArrayList<ContentProviderOperation> operationList, String owner)
    {
      Util.printMethodName(TAG);
      Uri uri = GenericURIs.URI_FILES.buildUpon()
          .appendQueryParameter(GenericContract.CALLER_IS_SYNC_ADAPTER, "true")
          .build();
      for( FileMetaData_ShortInfo_PB mShort : newFiles )
      {
        try
        {
          updateSyncNotification(mContext, mBuilder,
              "Downloading new file: " + mShort.getFileName());
          FileMetaData_PB m = NetworkOps.getFileMetaData(mShort.getFileID());
          File newFile = new File(Utils.createRandomFileUri(mContext.getFilesDir())
              .getPath());
          long newSeq = NetworkOps.getFile(m.getFileID(), newFile);

          ContentProviderOperation.Builder b = ContentProviderOperation
              .newInsert(uri);
          b.withValue(MetaDataColumns.FILE_ID, m.getFileID())
              .withValue(MetaDataColumns.NAME, m.getFileName())
              .withValue(MetaDataColumns.TYPE, m.getFileType())
              .withValue(MetaDataColumns.OWNER, owner)
              .withValue(MetaDataColumns.SEQUENCE, newSeq)
              .withValue(
                  MetaDataColumns.TIME,
                  GenericContract.INTERNAL_DATE_FORMAT.format(translateDate(m
                      .getLastUpdated())))
              .withValue(MetaDataColumns.DIRTY, false)
              .withValue(MetaDataColumns.URI, Uri.fromFile(newFile).toString());
          operationList.add(b.build());
        }
        catch( Exception e )
        {
          ++mSyncResult.stats.numIoExceptions;
          e.printStackTrace();
        }
      }
    }

    public void addcreateOperations(
        ArrayList<ContentProviderOperation> operationList)
    {
      Util.printMethodName(TAG);
      for( MetaData m : toCreate )
      {
        try
        {
          updateSyncNotification(mContext, mBuilder, "Uploading new file: "+m.fileName());
          FileMetaData_PB m_pb = NetworkOps.createFile(new File(m.fileUri()
              .getPath()), m.fileId(), m.fileName(), m.fileType());
          ContentProviderOperation.Builder b = ContentProviderOperation
              .newUpdate(GenericURIs.URI_FILES
                  .buildUpon()
                  .appendPath(m.fileId())
                  .appendQueryParameter(GenericContract.CALLER_IS_SYNC_ADAPTER,
                      "true")
                  .appendQueryParameter(GenericContract.UNLOCK_FILE, "true")
                  .build());
          b.withValue(MetaDataColumns.SEQUENCE, m_pb.getSequenceNumber());
          b.withValue(MetaDataColumns.DIRTY, false);
          operationList.add(b.build());
        }
        catch( Exception e )
        {
          ++mSyncResult.stats.numIoExceptions;
          e.printStackTrace();
        }
      }
    }

    public void addUnlockUnrelatedOperations(
        ArrayList<ContentProviderOperation> operationList)
    {
      Util.printMethodName(TAG);
      for( MetaData m : untouched )
      {
        ContentProviderOperation.Builder b = ContentProviderOperation
            .newUpdate(GenericURIs.URI_FILES
                .buildUpon()
                .appendPath(m.fileId())
                .appendQueryParameter(GenericContract.CALLER_IS_SYNC_ADAPTER,
                    "true")
                .appendQueryParameter(GenericContract.UNLOCK_FILE, "true")
                .build());
        b.withValue(MetaDataColumns.SEQUENCE, m.sequenceNumber());
        operationList.add(b.build());
      }
    }

    public void addPushOperations(
        ArrayList<ContentProviderOperation> operationList)
    {
      Util.printMethodName(TAG);
      for( MetaData m : toPush )
      {
        try
        {
          updateSyncNotification(mContext, mBuilder, "Uploading: "+m.fileName());
          FileMetaData_PB m_pb = NetworkOps.overWriteFile(new File(m.fileUri()
              .getPath()), m.fileId(), m.fileName(), m.fileType());
          ContentProviderOperation.Builder b = ContentProviderOperation
              .newUpdate(GenericURIs.URI_FILES
                  .buildUpon()
                  .appendPath(m.fileId())
                  .appendQueryParameter(GenericContract.CALLER_IS_SYNC_ADAPTER,
                      "true")
                  .appendQueryParameter(GenericContract.UNLOCK_FILE, "true")
                  .build());
          b.withValue(MetaDataColumns.SEQUENCE, m_pb.getSequenceNumber());
          b.withValue(MetaDataColumns.DIRTY, false);
          operationList.add(b.build());
        }
        catch( Exception e )
        {
          ++mSyncResult.stats.numIoExceptions;
          e.printStackTrace();
        }
      }
    }

    public void addConflictOperations(
        ArrayList<ContentProviderOperation> operationList)
    {
      Util.printMethodName(TAG);
      updateSyncNotification(mContext, mBuilder, "Detecting conflicts");
      for( MetaData m : conflicts )
      {
        try
        {
          FileMetaData_PB m_pb = NetworkOps.getFileMetaData(m.fileId());
          ContentProviderOperation.Builder b = ContentProviderOperation
              .newInsert(GenericURIs.URI_BACKEND_CONFLICTS
                  .buildUpon()
                  .appendQueryParameter(GenericContract.CALLER_IS_SYNC_ADAPTER,
                      "true").build());
          b.withValue(BackendConflictColumns.FILE_ID, m.fileId())
              .withValue(BackendConflictColumns.FILE_OWNER, syncAccount.name)
              .withValue(BackendConflictColumns.BACKEND_SEQUENCE,
                  m_pb.getSequenceNumber())
              .withValue(
                  BackendConflictColumns.BACKEND_TIMESTAMP,
                  GenericContract.INTERNAL_DATE_FORMAT
                      .format(translateDate(m_pb.getLastUpdated())));
          operationList.add(b.build());
        }
        catch( Exception e )
        {
          ++mSyncResult.stats.numIoExceptions;
          e.printStackTrace();
        }
      }
    }
  }
}
