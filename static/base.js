function toggleMenu() {
  const menu = document.getElementById("navLinks");
  const toggle = document.querySelector(".menu-toggle");
  if (!menu || !toggle) return;

  const isActive = menu.classList.toggle("active");
  toggle.setAttribute("aria-expanded", String(isActive));
}

document.addEventListener("click", (event) => {
  const menu = document.getElementById("navLinks");
  const toggle = document.querySelector(".menu-toggle");
  if (!menu || !toggle) return;
  if (!menu.classList.contains("active")) return;

  const clickedToggle = toggle.contains(event.target);
  const clickedMenu = menu.contains(event.target);
  if (!clickedToggle && !clickedMenu) {
    menu.classList.remove("active");
    toggle.setAttribute("aria-expanded", "false");
  }
});

// LOUNGE BACKGROUND MUSIC
document.addEventListener("DOMContentLoaded", () => {
  const entryScreen = document.getElementById("loungeEntryScreen");
  const enterButton = document.getElementById("enterLoungeButton");
  const silentButton = document.getElementById("enterWithoutMusicButton");
  const playerFrame = document.getElementById("loungeSoundCloudPlayer");
  const control = document.getElementById("loungeMusicControl");

  if (
    !entryScreen ||
    !enterButton ||
    !silentButton ||
    !playerFrame ||
    !control
  ) {
    return;
  }

  const getStoredValue = (key) => {
    try {
      return localStorage.getItem(key);
    } catch (error) {
      return null;
    }
  };

  const setStoredValue = (key, value) => {
    try {
      localStorage.setItem(key, value);
    } catch (error) {
      // Local storage may be unavailable.
    }
  };

  const savedPreference = getStoredValue(
    "jukeboxLoungeMusicEnabled"
  );

  if (savedPreference !== null) {
    entryScreen.hidden = true;
    entryScreen.classList.add("is-hidden");
    control.hidden = false;
  }

  let widget = null;
  let widgetReady = false;
  let isPlaying = false;
  let pendingPlay = savedPreference === "yes";

  const updateControl = () => {
    const icon = control.querySelector("i");
    const label = control.querySelector("span");

    control.classList.toggle("is-paused", !isPlaying);
    control.setAttribute(
      "aria-label",
      isPlaying ? "Pause lounge music" : "Play lounge music"
    );
    control.setAttribute(
      "title",
      isPlaying ? "Pause lounge music" : "Play lounge music"
    );

    if (icon) {
      icon.className = isPlaying
        ? "fa-solid fa-pause"
        : "fa-solid fa-play";
    }

    if (label) {
      label.textContent = isPlaying ? "Music On" : "Music Off";
    }
  };

  const hideEntryScreen = () => {
    entryScreen.classList.add("is-hidden");

    window.setTimeout(() => {
      entryScreen.hidden = true;
    }, 500);
  };

  const savePlaybackPosition = () => {
    if (!widgetReady) {
      return;
    }

    widget.getPosition((position) => {
      if (Number.isFinite(position)) {
        setStoredValue(
          "jukeboxLoungeMusicPosition",
          String(position)
        );
      }
    });
  };

  const playLoungeMusic = () => {
    setStoredValue("jukeboxLoungeMusicEnabled", "yes");
    control.hidden = false;
    pendingPlay = true;

    if (widgetReady) {
      widget.play();
    }
  };

  const pauseLoungeMusic = (disablePreference = false) => {
    pendingPlay = false;

    if (disablePreference) {
      setStoredValue("jukeboxLoungeMusicEnabled", "no");
    }

    if (widgetReady) {
      savePlaybackPosition();
      widget.pause();
    }

    isPlaying = false;
    updateControl();
  };

  window.pauseLoungeBackgroundMusic = () => {
    pauseLoungeMusic(false);
  };

  window.resumeLoungeBackgroundMusic = () => {
    playLoungeMusic();
  };

  const initializeSoundCloudWidget = () => {
    if (!window.SC || !window.SC.Widget) {
      window.setTimeout(initializeSoundCloudWidget, 100);
      return;
    }

    widget = window.SC.Widget(playerFrame);
    window.loungeSoundCloudWidget = widget;

    widget.bind(window.SC.Widget.Events.READY, () => {
      widgetReady = true;
    widget.setVolume(55);

    // Start Juke Joint Love at the hook: 50 seconds.
    widget.seekTo(50000);

    if (pendingPlay) {
      widget.play();
    }
  });

  widget.bind(window.SC.Widget.Events.PLAY, () => {
    pendingPlay = false;
    isPlaying = true;
    control.hidden = false;
    updateControl();
  });

  widget.bind(window.SC.Widget.Events.PAUSE, () => {
    isPlaying = false;
    updateControl();
  });

  widget.bind(window.SC.Widget.Events.FINISH, () => {
    const musicEnabled =
      getStoredValue("jukeboxLoungeMusicEnabled") === "yes";

    isPlaying = false;

    if (musicEnabled) {
      widget.seekTo(0);
      widget.play();
    } else {
      updateControl();
    }
  });

  enterButton.addEventListener("click", () => {
    hideEntryScreen();
    playLoungeMusic();
  });

  silentButton.addEventListener("click", () => {
    hideEntryScreen();
    control.hidden = false;
    pauseLoungeMusic(true);
  });

  control.addEventListener("click", () => {
    if (isPlaying) {
      pauseLoungeMusic(true);
    } else {
      playLoungeMusic();
    }
  });

    if (savedPreference !== null) {
      if (savedPreference === "yes") {
        playLoungeMusic();
      } else {
        updateControl();
      }
    }
  };

  initializeSoundCloudWidget();
});
