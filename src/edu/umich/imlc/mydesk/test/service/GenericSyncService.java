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
import edu.umich.imlc.mydesk.cloud.client.network.NetUtil;
import edu.umich.imlc.mydesk.cloud.client.network.NetworkOps;
import edu.umich.imlc.mydesk.cloud.client.utilities.Util;
import edu.umich.imlc.mydesk.test.common.GenericContract;
import edu.umich.imlc.mydesk.test.common.GenericContract.MetaData;
import edu.umich.imlc.mydesk.test.common.GenericContract.MetaDataColumns;
import edu.umich.imlc.mydesk.test.common.GenericContract.MetaDataProjections;
import edu.umich.imlc.mydesk.test.common.Utils;
import edu.umich.imlc.protocolbuffer.general.ProtocolBufferTransport.Date_PB;

import android.accounts.Account;
import android.app.Service;
import android.content.AbstractThreadedSyncAdapter;
import android.content.ContentProviderClient;
import android.content.ContentProviderOperation;
import android.content.Context;
import android.content.Intent;
import android.content.SyncResult;
import android.database.Cursor;
import android.net.Uri;
import android.os.Bundle;
import android.os.IBinder;
import android.os.RemoteException;
import android.util.Log;

public class GenericSyncService extends Service
{
  private static final String TAG = "GenericSync";
  private static GenericSyncAdapter syncAdapter = null;

  @Override
  public IBinder onBind(Intent arg0)
  {
    Util.printMethodName();
    return getSyncAdapter().getSyncAdapterBinder();
  }

  private GenericSyncAdapter getSyncAdapter()
  {
    Util.printMethodName();
    if( syncAdapter == null )
    {
      syncAdapter = new GenericSyncAdapter(this);
    }
    return syncAdapter;
  }

  private static class GenericSyncAdapter extends AbstractThreadedSyncAdapter
  {

    public GenericSyncAdapter(Context context)
    {
      super(context, true);
      Utils.printMethodName();
    }

    @Override
    public void onPerformSync(Account account, Bundle extras, String authority,
        ContentProviderClient provider, SyncResult syncResult)
    {
      Util.printMethodName();
      try
      {
        if( !NetUtil.isLoggedIn() )
        {
          new LoginTask(getContext().getApplicationContext(), account.name)
              .doLogin();
        }

        SyncTodos todos = new SyncTodos(getMetaDatas(account, provider),
            NetworkOps.getListMetaData());

        ArrayList<ContentProviderOperation> operationList = new ArrayList<ContentProviderOperation>();

        todos.addLockOperations(operationList);
        provider.applyBatch(operationList);
        operationList.clear();

        todos.addPullOperations(operationList, getContext()
            .getApplicationContext());
        provider.applyBatch(operationList);
        operationList.clear();

        todos.addNewFilesOperations(operationList, account.name, getContext()
            .getApplicationContext());
        provider.applyBatch(operationList);
        operationList.clear();

        todos.addcreateOperations(operationList);
        provider.applyBatch(operationList);
        operationList.clear();

        todos.addPushOperations(operationList);
        provider.applyBatch(operationList);
        operationList.clear();

        provider.update(GenericContract.URI_FILES.buildUpon()
            .appendQueryParameter(GenericContract.CLEAN_FILE, "true").build(),
            null, null, null);
      }
      catch( Exception e )
      {
        e.printStackTrace();
      }
    }

    private Map<String, MetaData> getMetaDatas(Account account,
        ContentProviderClient provider) throws RemoteException
    {
      Util.printMethodName();
      Cursor c = provider.query(GenericContract.URI_FILES.buildUpon()
          .appendQueryParameter(MetaDataColumns.OWNER, account.name).build(),
          MetaDataProjections.METADATA, MetaDataColumns.DIRTY + "=1", null,
          null);
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
    private ArrayList<MetaData> conflicts;
    private ArrayList<MetaData> toCreate;
    private ArrayList<MetaData> toPush;
    private ArrayList<MetaData> toPull;
    private ArrayList<FileMetaData_ShortInfo_PB> newFiles;

    public SyncTodos(Map<String, MetaData> local,
        List<FileMetaData_ShortInfo_PB> backend)
    {
      Util.printMethodName();
      Log.i(TAG, "local:\n"+local);
      Log.i(TAG, "Backend:\n"+backend);
      conflicts = new ArrayList<GenericContract.MetaData>();
      toCreate = new ArrayList<GenericContract.MetaData>();
      toPush = new ArrayList<GenericContract.MetaData>();
      toPull = new ArrayList<MetaData>();
      newFiles = new ArrayList<FileMetaData_ShortInfo_PB>();
      for( FileMetaData_ShortInfo_PB b : backend )
      {
        if( !local.containsKey(b.getFileID()) )
        {
          newFiles.add(b);
          continue;
        }
        MetaData m = local.remove(b.getFileID());
        if( m.dirty() )
        {
          if( m.sequenceNumber() <= b.getSequenceNumber() )
          {
            conflicts.add(m);
          }
          else
          {
            toPush.add(m);
          }
        }
        else
        {
          if( m.sequenceNumber() < b.getSequenceNumber() )
          {
            toPull.add(m);
          }
        }
      }
      for( MetaData m : local.values() )
      {
        if( m.dirty() )
        {
          toCreate.add(m);
        }
      }
      
      Log.i(TAG, "conflicts:\n"+conflicts);
      Log.i(TAG, "toCreate:\n" + toCreate);
      Log.i(TAG, "toPush:\n"+ toPush);
      Log.i(TAG, "toPull:\n"+ toPull);
      Log.i(TAG, "newFiles:\n"+ newFiles);
    }

    public void addPullOperations(
        ArrayList<ContentProviderOperation> operationList, Context c)
    {
      Util.printMethodName();
      for( MetaData m : toPull )
      {
        try
        {
          File newFile = new File(Utils.createRandomFileUri(c.getFilesDir())
              .getPath());
          FileMetaData_PB m_pb = NetworkOps.getFileMetaData(m.fileId());
          long newSeq = NetworkOps.getFile(m.fileId(), newFile);
          ContentProviderOperation.Builder b = ContentProviderOperation
              .newUpdate(GenericContract.URI_FILES
                  .buildUpon()
                  .appendPath(m.fileId())
                  .appendQueryParameter(GenericContract.CALLER_IS_SYNC_ADAPTER,
                      "true").build());
          b.withValue(GenericContract.KEY_NEW_FILE, newFile.toString())
              .withValue(GenericContract.KEY_UPDATE_OLD_SEQUENCE,
                  m.sequenceNumber())
              .withValue(MetaDataColumns.SEQUENCE, newSeq)
              .withValue(GenericContract.KEY_UPDATE_BACKEND, true)
              .withValue(
                  MetaDataColumns.TIME,
                  DateFormat.getDateTimeInstance().format(
                      translateDate(m_pb.getLastUpdated())));
          operationList.add(b.build());
        }
        catch( Exception e )
        {
          e.printStackTrace();
        }
      }
    }

    public void addNewFilesOperations(
        ArrayList<ContentProviderOperation> operationList, String owner,
        Context c)
    {
      Util.printMethodName();
      Uri uri = GenericContract.URI_FILES.buildUpon()
          .appendQueryParameter(GenericContract.CALLER_IS_SYNC_ADAPTER, "true")
          .build();
      for( FileMetaData_ShortInfo_PB mShort : newFiles )
      {
        try
        {
          FileMetaData_PB m = NetworkOps.getFileMetaData(mShort.getFileID());
          File newFile = new File(Utils.createRandomFileUri(c.getFilesDir())
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
                  DateFormat.getDateTimeInstance().format(
                      translateDate(m.getLastUpdated())))
              .withValue(MetaDataColumns.DIRTY, false)
              .withValue(MetaDataColumns.URI, Uri.fromFile(newFile).toString());
          operationList.add(b.build());
        }
        catch( Exception e )
        {
          e.printStackTrace();
        }
      }
    }

    public void addcreateOperations(
        ArrayList<ContentProviderOperation> operationList)
    {
      Util.printMethodName();
      for( MetaData m : toCreate )
      {
        try
        {
          FileMetaData_PB m_pb = NetworkOps.createFile(new File(m.fileUri()
              .getPath()), m.fileId(), m.fileName(), m.fileType());
          ContentProviderOperation.Builder b = ContentProviderOperation
              .newUpdate(GenericContract.URI_FILES
                  .buildUpon()
                  .appendPath(m.fileId())
                  .appendQueryParameter(GenericContract.CALLER_IS_SYNC_ADAPTER,
                      "true")
                  .appendQueryParameter(GenericContract.CLEAN_FILE, "true")
                  .build());
          b.withValue(MetaDataColumns.SEQUENCE, m_pb.getSequenceNumber());
          operationList.add(b.build());
        }
        catch( Exception e )
        {
          e.printStackTrace();
        }
      }
    }

    public void addLockOperations(
        ArrayList<ContentProviderOperation> operationList)
    {
      Util.printMethodName();
      for( MetaData m : toCreate )
      {
        ContentProviderOperation.Builder b = ContentProviderOperation
            .newUpdate(GenericContract.URI_FILES
                .buildUpon()
                .appendPath(m.fileId())
                .appendQueryParameter(GenericContract.CALLER_IS_SYNC_ADAPTER,
                    "true")
                .appendQueryParameter(GenericContract.LOCK_FILE, "true")
                .build());
        b.withValue(MetaDataColumns.LOCKED, true);
        operationList.add(b.build());
      }
      for( MetaData m : toPush )
      {
        ContentProviderOperation.Builder b = ContentProviderOperation
            .newUpdate(GenericContract.URI_FILES
                .buildUpon()
                .appendPath(m.fileId())
                .appendQueryParameter(GenericContract.CALLER_IS_SYNC_ADAPTER,
                    "true")
                .appendQueryParameter(GenericContract.LOCK_FILE, "true")
                .build());
        b.withValue(MetaDataColumns.LOCKED, true);
        operationList.add(b.build());
      }
    }

    public void addPushOperations(
        ArrayList<ContentProviderOperation> operationList)
    {
      Util.printMethodName();
      for( MetaData m : toPush )
      {
        try
        {
          FileMetaData_PB m_pb = NetworkOps.overWriteFile(new File(m.fileUri()
              .getPath()), m.fileId(), m.fileName(), m.fileType());
          ContentProviderOperation.Builder b = ContentProviderOperation
              .newUpdate(GenericContract.URI_FILES
                  .buildUpon()
                  .appendPath(m.fileId())
                  .appendQueryParameter(GenericContract.CALLER_IS_SYNC_ADAPTER,
                      "true")
                  .appendQueryParameter(GenericContract.CLEAN_FILE, "true")
                  .build());
          b.withValue(MetaDataColumns.SEQUENCE, m_pb.getSequenceNumber());
          operationList.add(b.build());
        }
        catch( Exception e )
        {
          e.printStackTrace();
        }
      }
    }

    private Date translateDate(Date_PB dPB)
    {
      Calendar c = Calendar.getInstance();
      c.set(dPB.getYear() - 1900, dPB.getMonth() - 1, dPB.getDay(), dPB
          .getTime().getHours(), dPB.getTime().getMinutes(), dPB.getTime()
          .getSeconds());
      return c.getTime();
    }
  }
}
