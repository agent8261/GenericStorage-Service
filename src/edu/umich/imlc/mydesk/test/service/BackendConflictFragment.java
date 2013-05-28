package edu.umich.imlc.mydesk.test.service;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;

import edu.umich.imlc.mydesk.test.common.GenericContract.BackendConflictColumns;
import edu.umich.imlc.mydesk.test.common.GenericContract.BackendResolve;
import edu.umich.imlc.mydesk.test.common.GenericContract;
import edu.umich.imlc.mydesk.test.common.GenericContract.MetaData;
import edu.umich.imlc.mydesk.test.common.Utils;
import edu.umich.imlc.mydesk.test.common.GenericContract.GenericURIs;
import edu.umich.imlc.mydesk.test.common.GenericContract.MetaDataColumns;
import edu.umich.imlc.mydesk.test.common.GenericContract.Tables;
import android.app.AlertDialog;
import android.content.ContentValues;
import android.content.DialogInterface;
import android.database.Cursor;
import android.net.Uri;
import android.os.Bundle;
import android.support.v4.app.ListFragment;
import android.support.v4.app.LoaderManager.LoaderCallbacks;
import android.support.v4.content.CursorLoader;
import android.support.v4.content.Loader;
import android.support.v4.widget.SimpleCursorAdapter;
import android.view.View;
import android.widget.ListView;

public class BackendConflictFragment extends ListFragment implements
    LoaderCallbacks<Cursor>
{
  public static final String TAG = BackendConflictFragment.class
      .getSimpleName();
  public static final String[] mFromColumns = {
      Tables.BACKEND_CONFLICTS + "." + BackendConflictColumns.ID,
      MetaDataColumns.NAME };
  public static final int[] mToFields = { android.R.id.text1,
      android.R.id.text2 };
  public static final String[] loaderProjection = {
      Tables.BACKEND_CONFLICTS + "." + BackendConflictColumns.ID,
      MetaDataColumns.NAME,
      Tables.BACKEND_CONFLICTS + "." + BackendConflictColumns.FILE_ID,
      Tables.BACKEND_CONFLICTS + "." + BackendConflictColumns.BACKEND_TIMESTAMP };
  SimpleCursorAdapter mAdapter;
  Cursor mCursor = null;
  private static final int BACKEND_CONFLICT_LOADER = 0;

  /* (non-Javadoc)
   * @see android.support.v4.app.Fragment#onActivityCreated(android.os.Bundle)
   */
  @Override
  public void onActivityCreated(Bundle savedInstanceState)
  {
    Utils.printMethodName(TAG);
    mAdapter = new SimpleCursorAdapter(getActivity(),
        android.R.layout.two_line_list_item, null, mFromColumns, mToFields, 0);
    getLoaderManager().initLoader(BACKEND_CONFLICT_LOADER, null, this);
    setListAdapter(mAdapter);
    setEmptyText("No Conflicts");
    setHasOptionsMenu(false);
    super.onActivityCreated(savedInstanceState);
  }

  @Override
  public void onListItemClick(ListView l, View v, int position, long id)
  {
    if( mCursor.moveToPosition(position) )
    {
      displayResolutionDialog(mCursor);
    }
  }

  @Override
  public Loader<Cursor> onCreateLoader(int id, Bundle args)
  {
    Utils.printMethodName(TAG);
    switch ( id )
    {
      case BACKEND_CONFLICT_LOADER:
        CursorLoader loader = new CursorLoader(getActivity());
        loader.setUri(GenericURIs.URI_BACKEND_CONFLICTS);
        loader.setProjection(loaderProjection);
        loader.setSelection(Tables.BACKEND_CONFLICTS + "."
            + BackendConflictColumns.RESOLVED + "='"
            + BackendResolve.UNRESOLVED + "'");
        return loader;
      default:
        return null;
    }
  }

  @Override
  public void onLoadFinished(Loader<Cursor> loader, Cursor cursor)
  {
    Utils.printMethodName(TAG);
    mCursor = cursor;
    mAdapter.changeCursor(mCursor);
  }

  @Override
  public void onLoaderReset(Loader<Cursor> arg0)
  {
    Utils.printMethodName(TAG);
    mCursor = null;
    mAdapter.changeCursor(mCursor);
  }

  private void displayResolutionDialog(Cursor c)
  {
    final long conflictId = c.getLong(0);
    String fileId = c.getString(2);
    Date backendTimeStamp = null;
    try
    {
      backendTimeStamp = GenericContract.INTERNAL_DATE_FORMAT.parse(c
          .getString(3));
    }
    catch( ParseException e )
    {
      e.printStackTrace();
    }
    MetaData fileMetaData = null;
    Cursor metadataCursor = getActivity().getContentResolver().query(
        GenericURIs.URI_FILES.buildUpon().appendPath(fileId).build(),
        MetaDataColumns.METADATA_PROJ, null, null, null);
    if( metadataCursor.moveToFirst() )
    {
      fileMetaData = new MetaData(metadataCursor);
    }
    AlertDialog.Builder b = new AlertDialog.Builder(getActivity());
    b.setMessage("Choose the file version to keep:");
    String[] choices = {
        "Local file, last modified "
            + DateFormat.getDateTimeInstance().format(fileMetaData.timestamp())
            + "("
            + (fileMetaData.timestamp().after(backendTimeStamp) ? "newer)"
                : "older)"),
        "Backend file, last modified "
            + DateFormat.getDateTimeInstance().format(backendTimeStamp)
            + "("
            + (fileMetaData.timestamp().after(backendTimeStamp) ? "older)"
                : "newer)") };
    b.setSingleChoiceItems(choices, -1, new DialogInterface.OnClickListener()
    {
      @Override
      public void onClick(DialogInterface dialog, int which)
      {
        dialog.dismiss();
        Uri uri = GenericURIs.URI_BACKEND_CONFLICTS.buildUpon()
            .appendPath(String.valueOf(conflictId)).build();
        ContentValues values = new ContentValues();
        values.put(BackendConflictColumns.ID, conflictId);
        if( which == 0 )
        {
          // local
          values.put(BackendConflictColumns.RESOLVED,
              BackendResolve.LOCAL.name());
        }
        else if( which == 1 )
        {
          // backend
          values.put(BackendConflictColumns.RESOLVED,
              BackendResolve.BACKEND.name());
        }
        getActivity().getContentResolver().update(uri, values, null, null);
      }
    });
  }
}
