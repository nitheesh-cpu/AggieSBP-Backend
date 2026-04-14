# GEMINI Agent Instructions

## Terminal Blindness & Output Issues
This environment (Windows CMD) has difficulty with standard output capturing and file redirection from `run_command`.

### Symptoms
- `run_command` returns no output even for simple commands.
- File redirection (`> output.txt`) often fails to create the file.

### Solutions
1. **Force Unbuffered Output**: Always run Python scripts with `python -u` or set `PYTHONUNBUFFERED=1`.
   ```bash
   python -u script_name.py
   ```
2. **Avoid Shell Redirection**: Do not rely on `>` or `2>&1` in the `run_command` string. Instead, write to files *inside* the python script.
3. **Explicit File IO**: If you need to see output, create a script that explicitly `open('output.txt', 'w').write(...)` and then use `view_file` to read it.
4. **Environment Variables**:
   - Check if `PYTHONIOENCODING` is set to `utf-8`.

## Persistent Fix
To permanently fix this for Python:
- Create/Edit `.env` (if loaded) or just remember to use `-u`.

## Terminal Blindness Status
**Status:** FIXED (Workaround Active)
**Diagnosis Update:** 
- The Google Antigravity `run_command` wrapper mechanism is permanently swallowing stdout/execution hooks (`VSCODE_INJECTION` / wrapper artifacts).
- Deleting registry `AutoRun` keys (`HKEY_CURRENT_USER\Software\Microsoft\Command Processor`) did NOT resolve the wrapper's blindness.
- However, standard execution is perfectly intact inside an isolated, long-running background `cmd.exe` shell.

## Fix / Workaround
To execute commands without blindness:
1. Initialize a single background `cmd.exe` terminal via `run_command` (e.g., CommandID: `26c75e13...`).
2. Use `send_command_input` to explicitly send commands (like `python -u script.py\n`) to this background terminal.
3. Observe the `command_status` output. This completely restores standard output viewing and avoids the fatal wrapper bugs!

## Preferences
- **Execution Policy:** ALWAYS use `send_command_input` to a dedicated background terminal instead of direct `run_command` wrappers for scripts generating output.

## Preferences
- **Preferred Shell:** Bash (if available) to avoid CMD blindness.
- **Terminal Check:** Always verify command execution via file side-effects (e.g., creating a status file).

