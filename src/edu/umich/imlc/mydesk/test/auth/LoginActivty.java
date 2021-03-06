package edu.umich.imlc.mydesk.test.auth;

import edu.umich.imlc.mydesk.cloud.android.auth.LoginCallback;
import edu.umich.imlc.mydesk.cloud.android.auth.LoginUtilities;
import edu.umich.imlc.mydesk.test.common.GenericContract;
import edu.umich.imlc.mydesk.test.common.Utils;
import edu.umich.imlc.mydesk.test.common.GenericContract.GenericURIs;
import android.accounts.Account;
import android.app.Activity;
import android.app.AlertDialog;
import android.app.ProgressDialog;
import android.content.ContentResolver;
import android.content.DialogInterface;
import android.content.DialogInterface.OnDismissListener;
import android.content.Intent;
import android.content.SharedPreferences;
import android.os.Bundle;

public class LoginActivty extends Activity implements LoginCallback
{
  public static final String TAG = "LoginActivity";
  ProgressDialog prog;
  SharedPreferences prefs;

  /*
   * (non-Javadoc)
   * 
   * @see android.app.Activity#onActivityResult(int, int,
   * android.content.Intent)
   */
  @Override
  protected void onActivityResult(int requestCode, int resultCode, Intent data)
  {
    Utils.printMethodName(TAG);
    if( (resultCode == Activity.RESULT_OK) )
    {
      prog = new ProgressDialog(this);
      prog.setCancelable(false);
      prog.setTitle("Verifying Account");
      prog.show();
      LoginUtilities.doOnActivityResult(this, requestCode, resultCode, data,
          this);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see android.app.Activity#onStart()
   */
  @Override
  protected void onStart()
  {
    super.onStart();
    Utils.printMethodName(TAG);
    Intent i = getIntent();

    if( !i.getBooleanExtra(GenericContract.KEY_CHOOSE_ACCOUNT, false) )
    {
      if( getSharedPrefs().contains(GenericContract.PREFS_ACCOUNT_NAME) )
      {
        // account verified
        showAccountVerified(getSharedPrefs().getString(
            GenericContract.PREFS_ACCOUNT_NAME, ""));
        return;
      }
    }
    LoginUtilities.startAccountPicker(this, true);
  }

  private SharedPreferences getSharedPrefs()
  {
    Utils.printMethodName(TAG);
    if( prefs == null )
    {
      prefs = getSharedPreferences(GenericContract.SHARED_PREFS, MODE_PRIVATE);
    }
    return prefs;
  }

  @Override
  public void onSuccess(final String accountName)
  {
    Utils.printMethodName(TAG);

    // disable syncing for old account
    String oldAccountName = getSharedPrefs().getString(
        GenericContract.PREFS_ACCOUNT_NAME, null);
    if( oldAccountName != null )
    {
      Account oldAccount = new Account(oldAccountName,
          LoginUtilities.googleAccountType);
      ContentResolver.setSyncAutomatically(oldAccount,
          GenericContract.AUTHORITY, false);
    }

    // enable syncing of new account
    getSharedPrefs().edit()
        .putString(GenericContract.PREFS_ACCOUNT_NAME, accountName).apply();
    Account account = new Account(accountName, LoginUtilities.googleAccountType);
    ContentResolver.setSyncAutomatically(account, GenericContract.AUTHORITY,
        true);
    ContentResolver.addPeriodicSync(account, GenericContract.AUTHORITY,
        new Bundle(), 30 * 60);

    // notify any running cursors / loaders to refresh
    getApplicationContext().getContentResolver().notifyChange(
        GenericURIs.URI_BASE, null, true);
    getApplicationContext().getContentResolver().notifyChange(
        GenericURIs.URI_FILES, null, true);
    getApplicationContext().getContentResolver().notifyChange(
        GenericURIs.URI_CURRENT_ACCOUNT, null, true);
    getApplicationContext().getContentResolver().notifyChange(
        GenericURIs.URI_BACKEND_CONFLICTS, null, true);
    getApplicationContext().getContentResolver().notifyChange(
        GenericURIs.URI_LOCAL_CONFLICTS, null, true);
    runOnUiThread(new Runnable()
    {

      @Override
      public void run()
      {
        prog.dismiss();
        showAccountVerified(accountName);
      }
    });
  }

  @Override
  public void onFailure(final Exception e)
  {
    Utils.printMethodName(TAG);
    e.printStackTrace();
    runOnUiThread(new Runnable()
    {
      @Override
      public void run()
      {
        prog.dismiss();
        showVerifyFail(e);
      }
    });

  }

  private void showAccountVerified(String accountName)
  {
    Utils.printMethodName(TAG);
    AlertDialog alert  = new AlertDialog.Builder(this).create();
    alert.setMessage("Account verified: " + accountName);
    alert.setOnDismissListener(new OnDismissListener()
    {
      @Override
      public void onDismiss(DialogInterface dialog)
      {
        finish();
      }
    });
    alert.show();
  }

  private void showVerifyFail(Throwable e)
  {
    Utils.printMethodName(TAG);
    AlertDialog alert = new AlertDialog.Builder(this).create();
    alert.setMessage("Verify failed: " + e);
    alert.setOnDismissListener(new OnDismissListener()
    {
      @Override
      public void onDismiss(DialogInterface dialog)
      {
        finish();
      }
    });
    alert.show();
  }
}
