package com.pedro.rtspserver

import android.content.Context
import android.media.MediaCodec
import android.media.MediaFormat
import android.os.Build
import android.util.Log
import androidx.annotation.RequiresApi
import com.pedro.common.AudioCodec
import com.pedro.common.ConnectChecker
import com.pedro.common.VideoCodec
import com.pedro.encoder.video.GetVideoData
import com.pedro.encoder.video.VideoEncoder
import com.pedro.library.base.Camera2Base
import com.pedro.library.view.OpenGlView
import com.pedro.rtspserver.server.RtspServer
import com.pedro.rtspserver.util.RtspServerStreamClient
import java.nio.ByteBuffer

@RequiresApi(api = Build.VERSION_CODES.LOLLIPOP)
class RtspServerCamera2: Camera2Base {

  private val rtspServer: RtspServer

  // ---------------------------------------------------------------- sub stream
  // RecyCam拡張: 低解像度サブストリーム（別ポートの第2RTSPサーバー）。
  //
  // RootEncoder の Camera2Base には「録画用第2エンコーダ」(videoEncoderRecord,
  // protected) が用意されており、prepareVideo の recordWidth/recordHeight/
  // recordBitrate を本配信と別解像度にすると differentRecordResolution=true となって
  // GLパイプラインが第2サーフェスにも描画する。本クラスではこの第2エンコーダを
  // 自前コールバック付きインスタンスへ差し替え、出力を第2 RtspServer に注入する。
  // 準備・開始・停止・GL結線・requestKeyFrame は Camera2Base がすべて面倒を見る。
  //
  // 注意（既知のトレードオフ）:
  // - differentRecordResolution=true の間、Camera2Base はメインエンコーダ出力を
  //   recordController に渡さない。そのため startRecord（配信元録画）は本クラスの
  //   subVideoData 経由で「サブ解像度」で記録される。
  // - サブストリームのコーデックは互換性と受信負荷を優先して H264 固定。
  private var subServer: RtspServer? = null
  private var subStreamConnectChecker: ConnectChecker? = null
  private var subStreamPort = -1

  constructor(openGlView: OpenGlView, connectChecker: ConnectChecker, port: Int): super(openGlView) {
    rtspServer = RtspServer(connectChecker, port)
  }

  constructor(context: Context, connectCheckerRtsp: ConnectChecker, port: Int): super(context) {
    rtspServer = RtspServer(connectCheckerRtsp, port)
  }

  fun startStream() {
    super.startStream("")
  }

  /**
   * サブストリームを有効化する。**prepareVideo より前に呼ぶこと。**
   *
   * 呼び出し側は prepareVideo の 11引数版で recordWidth/recordHeight/recordBitrate に
   * サブ解像度・ビットレートを渡す（メインとアスペクト比一致が必須。
   * 不一致だと prepareVideo が false を返す）。
   *
   * @param port サブ配信のポート（例: メイン+1）
   * @param connectChecker サブサーバー専用のイベント通知先
   *        （メインと混ざらないよう別インスタンス推奨）
   */
  fun enableSubStream(port: Int, connectChecker: ConnectChecker) {
    if (isStreaming) {
      Log.e(TAG, "enableSubStream: must be called before startStream")
      return
    }
    subStreamPort = port
    subStreamConnectChecker = connectChecker
    subServer = RtspServer(connectChecker, port).also {
      // サブはH264固定（受信側のデコード負荷・互換性を優先）。
      it.setVideoCodec(VideoCodec.H264)
    }
    // 第2エンコーダを自前コールバック付きに差し替える（Camera2Base.videoEncoderRecord
    // は protected）。以降の prepare/start/stop/GL結線は基底クラスが行う。
    videoEncoderRecord = VideoEncoder(subVideoData)
    Log.i(TAG, "sub stream enabled on port $port")
  }

  /** サブストリームを無効化する。配信停止中のみ有効。 */
  fun disableSubStream() {
    if (isStreaming) {
      Log.e(TAG, "disableSubStream: must be called while not streaming")
      return
    }
    try { subServer?.stopServer() } catch (_: Throwable) {}
    subServer = null
    subStreamConnectChecker = null
    subStreamPort = -1
  }

  fun isSubStreamEnabled(): Boolean = subServer != null

  fun getSubStreamPort(): Int = subStreamPort

  /** サブサーバーの接続クライアント数などにアクセスするためのクライアント。 */
  fun getSubStreamClient(): RtspServerStreamClient? =
    subServer?.let { RtspServerStreamClient(it) }

  /**
   * メイン・サブ両方のサーバーへ認証情報を適用する。
   * （アプリ側はreflectionで `camera.setAuth` を探すため、このメソッドが直接使われる。）
   */
  fun setAuth(user: String?, password: String?) {
    rtspServer.setAuth(user, password)
    subServer?.setAuth(user, password)
  }

  /** サブエンコーダの出力を第2RTSPサーバーへ注入するコールバック。 */
  private val subVideoData = object : GetVideoData {
    override fun onVideoInfo(sps: ByteBuffer, pps: ByteBuffer?, vps: ByteBuffer?) {
      subServer?.setVideoInfo(sps.duplicate(), pps?.duplicate(), vps?.duplicate())
    }

    override fun getVideoData(videoBuffer: ByteBuffer, info: MediaCodec.BufferInfo) {
      subServer?.sendVideo(videoBuffer.duplicate(), info)
      // differentRecordResolution=true の間はメイン側が recordController へ渡さないため、
      // 配信元録画（startRecord）はここから記録する（サブ解像度になる）。
      recordController.recordVideo(videoBuffer, info)
    }

    override fun onVideoFormat(mediaFormat: MediaFormat) {
      recordController.setVideoFormat(mediaFormat)
    }
  }

  // ---------------------------------------------------------------- overrides

  override fun onAudioInfoImp(isStereo: Boolean, sampleRate: Int) {
    rtspServer.setAudioInfo(sampleRate, isStereo)
    subServer?.setAudioInfo(sampleRate, isStereo)
  }

  override fun startStreamImp(url: String) {
    rtspServer.startServer()
    subServer?.startServer()
  }

  override fun stopStreamImp() {
    rtspServer.stopServer()
    subServer?.stopServer()
  }

  override fun getAudioDataImp(audioBuffer: ByteBuffer, info: MediaCodec.BufferInfo) {
    // sendAudio はバッファのposition を消費し得るため、サブには複製を渡す。
    subServer?.sendAudio(audioBuffer.duplicate(), info)
    rtspServer.sendAudio(audioBuffer, info)
  }

  override fun onVideoInfoImp(sps: ByteBuffer, pps: ByteBuffer?, vps: ByteBuffer?) {
    val newSps = sps.duplicate()
    val newPps = pps?.duplicate()
    val newVps = vps?.duplicate()
    rtspServer.setVideoInfo(newSps, newPps, newVps)
    // サブの SPS/PPS は subVideoData.onVideoInfo（サブエンコーダ自身の出力）から設定される。
  }

  override fun getVideoDataImp(videoBuffer: ByteBuffer, info: MediaCodec.BufferInfo) {
    rtspServer.sendVideo(videoBuffer, info)
  }

  override fun getStreamClient(): RtspServerStreamClient = RtspServerStreamClient(rtspServer)

  override fun setVideoCodecImp(codec: VideoCodec) {
    rtspServer.setVideoCodec(codec)
    // サブはH264固定のため subServer には反映しない。
  }

  override fun setAudioCodecImp(codec: AudioCodec) {
    rtspServer.setAudioCodec(codec)
    subServer?.setAudioCodec(codec)
  }

  companion object {
    private const val TAG = "RtspServerCamera2"
  }
}
