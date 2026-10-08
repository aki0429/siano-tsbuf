/*
 * siano-tsbuf — PX-Q1UD / PX-S1UD (Siano, /dev/dvb) 向け TS 受信安定化ラッパ
 *
 * 使い方:
 *   siano-tsbuf [-b MiB] [-l logfile] [-t tag] -- dvbv5-zap -a N -c CONF -r -P -o - CH
 *
 * 背景 (2026-10-08 実測):
 *   dvbv5-zap は 1 スレッドで「dvr0 を read → stdout へ write」を繰り返す。
 *   stdout の先 (Mirakurun B / Node) のイベントループが数秒止まると write がブロックし、
 *   その間 dvr0 を読めなくなる。カーネルの DVR リングバッファは 6MB (= 地デジ約3秒) しか無く、
 *   溢れると "buffer overrun" となって連続した TS 塊が失われ、全 PID で同時に CC ドロップが出る。
 *   実測: 読み手が 2 秒止まるだけで 20 秒間に 70 ドロップ、止めなければ 0 ドロップ。
 *   EDCB の .err でも Q1UD のドロップは「信号良好 (C/N 16dB) なのに PID 数ぶん (~15) の
 *   ドロップが同時刻に複数チューナーで発生」しており、RF ではなくこの溢れが主因。
 *
 * 対策:
 *   1. 受信専用スレッドが pipe から常に読み続け、上限付きユーザー空間リング (既定 32MiB ≒ 地デジ15秒,
 *      -b で 4..256MiB)
 *      に溜める。送信スレッドが下流へ書く。下流が止まっても dvbv5-zap の write はブロックせず、
 *      DVR バッファは溢れない。リングが満杯になった時だけ 188 バイト境界で新着を捨てて記録する。
 *      子の終了時に残った 188 未満の端数も捨てずにそのまま出力する (バイト透過)。
 *   2. 子 (dvbv5-zap) に PR_SET_PDEATHSIG を設定し、本プロセスが SIGKILL されても
 *      frontend を掴んだ孤児を残さない (2026-10-01 の全滅障害の再発防止)。
 *      本プロセス自身も親 (dvb5-loop) が死んだら SIGTERM を受ける。
 *   3. root 実行時は nice -10 に上げ、高負荷時に読み出しが遅れないようにする (権限が無ければ何もしない)。
 *   4. dvbv5-zap の stderr を監視し "buffer overrun" やリング溢れをログに残す (可観測性)。
 *
 * 終了コード: 子の終了コードをそのまま返す (シグナル終了は 128+sig)。
 */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/prctl.h>
#include <sys/resource.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define TS 188

/* ---- 設定 ---- */
static size_t ring_size = 32u << 20;
static const char *log_path = "-";
static char tag[128] = "";

/* ---- 共有状態 ---- */
static uint8_t *ring;
static size_t r_head, r_tail, r_used; /* head=書込位置, tail=読出位置 */
static pthread_mutex_t mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t cv_data = PTHREAD_COND_INITIALIZER;
static int in_eof, out_dead;
static uint64_t bytes_in, bytes_out, bytes_dropped, drop_events, overruns;
static size_t peak_used;
static pid_t child = -1;
static volatile sig_atomic_t got_sig;
static FILE *logf;
static pthread_mutex_t log_mu = PTHREAD_MUTEX_INITIALIZER;
static time_t t_start;

static void logmsg(const char *fmt, ...) {
	if (!logf) return;
	char ts[32];
	time_t now = time(NULL);
	struct tm tm;
	localtime_r(&now, &tm);
	strftime(ts, sizeof ts, "%Y-%m-%d %H:%M:%S", &tm);
	pthread_mutex_lock(&log_mu);
	fprintf(logf, "[%s] %s pid=%d ", ts, tag, (int)getpid());
	va_list ap;
	va_start(ap, fmt);
	vfprintf(logf, fmt, ap);
	va_end(ap);
	fputc('\n', logf);
	fflush(logf);
	pthread_mutex_unlock(&log_mu);
}

static void on_sig(int s) {
	got_sig = s;
	if (child > 0) kill(child, SIGTERM);
}

/* pipe から読み続けるスレッド: 下流の状態に関係なく決してブロックしない */
static void *reader(void *arg) {
	int fd = (int)(intptr_t)arg;
	size_t bufsz = 1 << 18;
	uint8_t *buf = malloc(bufsz);
	if (!buf) return NULL;
	size_t carry = 0; /* 188 境界に揃えるための端数 */
	for (;;) {
		if (carry >= TS) carry = 0; /* 不変条件: 端数は常に 188 未満 */
		ssize_t n = read(fd, buf + carry, bufsz - carry);
		if (n < 0 && errno == EINTR) continue;
		if (n < 0) break;
		/* EOF にだけ端数をそのまま通す。正常な入力の末尾を TS 単位へ丸めて捨てない。 */
		if (n == 0 && carry == 0) break;
		size_t have = carry + (size_t)n;
		size_t whole = n == 0 ? have : have - have % TS;
		pthread_mutex_lock(&mu);
		bytes_in += whole;
		size_t freesp = ring_size - r_used;
		size_t take = whole <= freesp ? whole : (freesp - freesp % TS);
		if (take < whole) {
			/* リング満杯: 下流が長時間停止。新着を捨てる (TS 境界単位) */
			bytes_dropped += whole - take;
			drop_events++;
		}
		size_t off = 0;
		while (off < take) {
			size_t chunk = take - off;
			size_t room = ring_size - r_head;
			if (chunk > room) chunk = room;
			memcpy(ring + r_head, buf + off, chunk);
			r_head = (r_head + chunk) % ring_size;
			off += chunk;
		}
		r_used += take;
		if (r_used > peak_used) peak_used = r_used;
		uint64_t dev = drop_events;
		int notify_drop = take < whole;
		pthread_cond_signal(&cv_data);
		pthread_mutex_unlock(&mu);
		if (notify_drop && (dev <= 5 || dev % 100 == 0))
			logmsg("ring full: dropped %zu bytes (events=%llu)", whole - take, (unsigned long long)dev);
		carry = have - whole; /* 常に TS(188) 未満 */
		if (carry >= TS) carry = 0;
		if (carry) memmove(buf, buf + whole, carry);
	}
	pthread_mutex_lock(&mu);
	in_eof = 1;
	pthread_cond_signal(&cv_data);
	pthread_mutex_unlock(&mu);
	free(buf);
	return NULL;
}

/* 下流 (stdout) に書くスレッド */
static void *writer(void *arg) {
	(void)arg;
	for (;;) {
		pthread_mutex_lock(&mu);
		while (r_used == 0 && !in_eof) pthread_cond_wait(&cv_data, &mu);
		if (r_used == 0 && in_eof) {
			pthread_mutex_unlock(&mu);
			break;
		}
		size_t chunk = r_used;
		size_t room = ring_size - r_tail;
		if (chunk > room) chunk = room;
		if (chunk > (1u << 20)) chunk = 1u << 20;
		uint8_t *p = ring + r_tail;
		pthread_mutex_unlock(&mu);

		/* ロック外で書く (この間 reader は空き領域にだけ書くので p の範囲は上書きされない) */
		size_t done = 0;
		while (done < chunk) {
			ssize_t w = write(STDOUT_FILENO, p + done, chunk - done);
			if (w < 0 && errno == EINTR) continue;
			if (w <= 0) {
				pthread_mutex_lock(&mu);
				out_dead = 1;
				pthread_mutex_unlock(&mu);
				if (child > 0) kill(child, SIGTERM);
				return NULL;
			}
			done += (size_t)w;
		}
		pthread_mutex_lock(&mu);
		r_tail = (r_tail + chunk) % ring_size;
		r_used -= chunk;
		bytes_out += chunk;
		/* 空になったら先頭へ巻き戻す。reader の memcpy はロック内なので競合しない。
		 * これで常駐メモリは「実際に溜まった最大量」程度に収まる (メモリ逼迫機への配慮) */
		if (r_used == 0) r_head = r_tail = 0;
		pthread_mutex_unlock(&mu);
	}
	return NULL;
}

/* 子の stderr を中継しつつ overrun を記録するスレッド */
static void *errpump(void *arg) {
	FILE *f = fdopen((int)(intptr_t)arg, "r");
	char line[1024];
	while (f && fgets(line, sizeof line, f)) {
		fputs(line, stderr);
		if (strstr(line, "overrun")) {
			pthread_mutex_lock(&mu);
			uint64_t o = ++overruns;
			pthread_mutex_unlock(&mu);
			line[strcspn(line, "\r\n")] = 0;
			if (o <= 20 || o % 100 == 0) logmsg("dvr overrun #%llu: %s", (unsigned long long)o, line);
		} else if (strstr(line, "ERROR") || strstr(line, "rror") || strstr(line, "busy")) {
			line[strcspn(line, "\r\n")] = 0;
			logmsg("child: %s", line);
		}
	}
	if (f) fclose(f);
	return NULL;
}

static void usage(void) {
	fprintf(stderr, "usage: siano-tsbuf [-b MiB] [-l logfile|-] [-t tag] -- command args...\n"
	        "  -b MiB: strict decimal integer, 4..256 (default 32)\n"
	        "  --help / --version: show information on stderr\n");
}

/* atoi の部分成功や桁溢れを許さず、容量の上限を割当て前に確定する。 */
static int capacity(const char *text) {
	unsigned int value = 0;
	if (!*text) return -1;
	for (const unsigned char *p = (const unsigned char *)text; *p; ++p) {
		if (*p < '0' || *p > '9' || value > 256) return -1;
		value = value * 10 + (*p - '0');
	}
	if (value < 4 || value > 256) return -1;
	ring_size = (size_t)value << 20;
	return 0;
}

int main(int argc, char **argv) {
	int i = 1;
	const char *e;
	if (argc == 2 && !strcmp(argv[1], "--help")) { usage(); return 0; }
	if (argc == 2 && !strcmp(argv[1], "--version")) {
		fprintf(stderr, "siano-tsbuf 0.1.0-beta.1\n");
		return 0;
	}
	if ((e = getenv("SIANO_TSBUF_MB")) && capacity(e) != 0) { usage(); return 2; }
	if ((e = getenv("SIANO_TSBUF_LOG"))) log_path = e;
	for (; i < argc; i++) {
		if (!strcmp(argv[i], "--")) { i++; break; }
		if (!strcmp(argv[i], "-b") && i + 1 < argc) {
			if (capacity(argv[++i]) != 0) { usage(); return 2; }
		}
		else if (!strcmp(argv[i], "-l") && i + 1 < argc) log_path = argv[++i];
		else if (!strcmp(argv[i], "-t") && i + 1 < argc) snprintf(tag, sizeof tag, "%s", argv[++i]);
		else { usage(); return 2; }
	}
	if (i >= argc) { usage(); return 2; }

	/* 親 (dvb5-loop) が消えたら自分も終わる */
	prctl(PR_SET_PDEATHSIG, SIGTERM);
	if (getppid() == 1) return 143;

	if (strcmp(log_path, "-") == 0) logf = stderr;
	else if (!(logf = fopen(log_path, "a"))) logf = NULL; /* 書けなければ黙ってログ無し */
	if (!tag[0]) {
		/* 既定タグ: adapter と ch を引数から拾う */
		const char *a = "?", *ch = argv[argc - 1];
		for (int k = i; k < argc - 1; k++) if (!strcmp(argv[k], "-a")) a = argv[k + 1];
		snprintf(tag, sizeof tag, "adapter%s/%s", a, ch);
	}

	ring = malloc(ring_size); /* 触るまで物理メモリは確保されない */
	if (!ring) { perror("malloc"); return 1; }

	/* 読み出しが遅れないよう優先度を上げる (root のときのみ有効、失敗は無視) */
	if (setpriority(PRIO_PROCESS, 0, -10) != 0) { /* non-root */ }

	signal(SIGPIPE, SIG_IGN);
	struct sigaction sa = {0};
	sa.sa_handler = on_sig;
	sigaction(SIGTERM, &sa, NULL);
	sigaction(SIGINT, &sa, NULL);
	sigaction(SIGHUP, &sa, NULL);

	int pout[2], perr[2];
	if (pipe2(pout, O_CLOEXEC) || pipe2(perr, O_CLOEXEC)) { perror("pipe"); return 1; }
	fcntl(pout[0], F_SETPIPE_SZ, 1 << 20); /* 失敗しても既定 64KiB で動く */

	child = fork();
	if (child < 0) { perror("fork"); return 1; }
	if (child == 0) {
		prctl(PR_SET_PDEATHSIG, SIGKILL); /* 孤児化して frontend を掴み続けない */
		dup2(pout[1], STDOUT_FILENO);
		dup2(perr[1], STDERR_FILENO);
		signal(SIGPIPE, SIG_DFL);
		execvp(argv[i], &argv[i]);
		_exit(127);
	}
	close(pout[1]);
	close(perr[1]);
	t_start = time(NULL);
	logmsg("start capacity_bytes=%zu cmd=%s", ring_size, argv[i]);

	pthread_t tr, tw, te;
	pthread_create(&tr, NULL, reader, (void *)(intptr_t)pout[0]);
	pthread_create(&tw, NULL, writer, NULL);
	pthread_create(&te, NULL, errpump, (void *)(intptr_t)perr[0]);

	int st = 0;
	for (;;) {
		pid_t w = waitpid(child, &st, 0);
		if (w == child) break;
		if (w < 0 && errno != EINTR) break;
	}
	pthread_join(tr, NULL);
	/* 子が終わったら溜まっている分を出し切る (下流が死んでいれば即終了) */
	if (!got_sig) pthread_join(tw, NULL);
	pthread_join(te, NULL);

	int rc = WIFEXITED(st) ? WEXITSTATUS(st) : 128 + (WIFSIGNALED(st) ? WTERMSIG(st) : 0);
	if (got_sig) rc = 128 + got_sig;
	logmsg("exit rc=%d secs=%ld in_bytes=%llu out_bytes=%llu peak_bytes=%zu dvr_overruns=%llu ring_drop_events=%llu dropped_bytes=%llu",
	       rc, (long)(time(NULL) - t_start), (unsigned long long)bytes_in,
	       (unsigned long long)bytes_out, peak_used, (unsigned long long)overruns,
	       (unsigned long long)drop_events, (unsigned long long)bytes_dropped);
	return rc;
}
