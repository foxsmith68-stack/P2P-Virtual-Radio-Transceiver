# P2P Virtual Radio Transceiver

A 100% decentralized, serverless Peer-to-Peer virtual radio transceiver for Windows. Set your callsign, dial across 100 kHz to 30 GHz, and stream live PTT voice audio with anyone tuned to your exact frequency.

## How to Setup and Run

1. Make sure you have Python 3.10+ installed on Windows.
2. Open Command Prompt and install the required libraries:
   ```bash
   pip install PyQt6 qasync
   pip install upgrade pyaudio
Download p2p_radio.py from this repository.

Run the application:

Bash
python p2p_radio.py

How to Use

Tuning: Use the VFO display, direct frequency input, or the + / - buttons to change frequencies.

PTT Voice: Hold down the Spacebar to transmit audio to other users on your frequency. Release to listen.

Station Directory: Double-click any active user in the directory to instantly tune to their frequency.

Click **Commit changes** to save it. Now your page looks professional and ready for users.

---

## Going Big: Turn it into an `.exe` (Optional)
If you don't want your friends to have to install Python or use the command line at all, you can compile your script into a standard Windows executable file (`.exe`). 

You can do this right now in your command prompt:
1. Install PyInstaller: `pip install pyinstaller`
2. Run this command in your radio folder: 
   ```bash
   pyinstaller --noconsole --onefile p2p_radio.py
Look inside the newly created dist folder. You will find a standalone p2p_radio.exe file. You can upload that file directly to your GitHub page under the "Releases" section, and people can just double-click it to launch the radio instantly!

Drop the link here once you set it up—it would be awesome to see people jumping onto the VFO dial!


PLEASE Report any bugs
