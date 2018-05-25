const Parsers = require("./Parsers");

const CURRENT_TIME_MILLIS = 1525305600000; // 2018-05-02 18:00:00 in UTC -6)

test('absolute dates are preserved after conversion', () => {
    Date.now = jest.fn(() => CURRENT_TIME_MILLIS); // Mock the current time
    expect(Parsers.IQL1Parser.convertQueryDateToAbsoluteIfValid("FROM jobsearch 2018-05-23 2018-05-22")).toBe("FROM jobsearch 2018-05-23 2018-05-22");
    expect(Parsers.IQL1Parser.convertQueryDateToAbsoluteIfValid("FROM jobsearch 2018-05-20 01:00 2018-05-22 23:00")).toBe("FROM jobsearch 2018-05-20 01:00 2018-05-22 23:00");
});

test('date conversion works correctly', () => {
    Date.now = jest.fn(() => CURRENT_TIME_MILLIS); // Mock the current time
    expect(Parsers.IQL1Parser.convertQueryDateToAbsoluteIfValid("FROM jobsearch today tomorrow")).toBe("FROM jobsearch 2018-05-02 2018-05-03");
    expect(Parsers.IQL1Parser.convertQueryDateToAbsoluteIfValid("FROM jobsearch yesterday today")).toBe("FROM jobsearch 2018-05-01 2018-05-02");
    expect(Parsers.IQL1Parser.convertQueryDateToAbsoluteIfValid("FROM jobsearch 1d 0d")).toBe("FROM jobsearch 2018-05-01 2018-05-02");

    expect(Parsers.IQL1Parser.convertQueryDateToAbsoluteIfValid("FROM jobsearch 1h 0h")).toBe("FROM jobsearch 2018-05-01 23:00 2018-05-02");
    expect(Parsers.IQL1Parser.convertQueryDateToAbsoluteIfValid("FROM jobsearch 24h 23h")).toBe("FROM jobsearch 2018-05-01 2018-05-01 01:00");
});